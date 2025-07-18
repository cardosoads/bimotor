<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\Client;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Str;
use Illuminate\Support\Facades\Validator;

class ReceiveDataController extends Controller
{
    public function store(Request $request)
    {
        Log::info('Recebendo requisição de sincronização', [
            'ip' => $request->ip(),
            'headers' => $request->headers->all(),
            'payload' => $request->all()
        ]);

        // Verificar se o conteúdo é JSON válido
        $raw = $request->json()->all();
        if (empty($raw)) {
            Log::error('Falha ao decodificar JSON', [
                'content' => $request->getContent(),
                'headers' => $request->headers->all()
            ]);
            return response()->json(['error' => 'JSON inválido'], 400);
        }

        $payloadCount = is_array($raw['payload'] ?? null) ? count($raw['payload']) : 0;
        Log::info('Payload JSON recebido', ['tables' => $payloadCount]);

        // Validação do payload
        $validator = Validator::make($raw, [
            'user_identifier' => 'required|string',
            'payload' => 'present|array',
            'structure' => 'nullable|array',
            'structure.*.columns' => 'nullable|array',
            'structure.*.columns.*.name' => 'required_with:structure|string',
            'structure.*.columns.*.type' => 'required_with:structure|string',
            'structure.*.columns.*.nullable' => 'boolean',
            'structure.*.indexes' => 'nullable|array',
            'structure.*.indexes.*.type' => 'in:primary,unique,index',
            'structure.*.indexes.*.column' => 'required_with:structure.*.indexes|string',
            'structure.*.indexes.*.name' => 'nullable|string'
        ]);

        if ($validator->fails()) {
            Log::error('Validação inválida', [
                'errors' => $validator->errors()->all(),
                'payload' => $raw
            ]);
            return response()->json(['errors' => $validator->errors()], 422);
        }

        $data = $validator->validated();

        try {
            // Conectar ao tenant
            $client = Client::where('id', $data['user_identifier'])
                ->orWhere('database_name', $data['user_identifier'])
                ->first();
            if (!$client) {
                Log::error('Cliente não encontrado', ['user_identifier' => $data['user_identifier']]);
                return response()->json(['error' => 'Cliente não encontrado'], 404);
            }

            // Estabelecer conexão com o tenant
            $conn = $this->connectToTenant($client);
            if (!$conn) {
                Log::error('Falha ao conectar ao tenant', ['database' => $client->database_name]);
                return response()->json(['error' => 'Falha ao conectar ao banco de dados do tenant'], 500);
            }

            // Iniciar transação
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, false);
            $conn->beginTransaction();
            Log::info('Transação iniciada', ['database' => $client->database_name]);

            $insertedRecords = [];

            // Processar estrutura da tabela, se fornecida
            if (!empty($data['structure'])) {
                foreach ($data['structure'] as $rawTable => $structure) {
                    $table = $this->sanitizeTableName($rawTable);
                    Log::info('Processando estrutura da tabela', ['table' => $table]);

                    $columnTypes = $this->mapStructureToTypes($structure);

                    if (!Schema::connection('tenant')->hasTable($table)) {
                        Log::info('Criando nova tabela', ['table' => $table]);
                        $this->createTableWithTypes($table, $columnTypes, $structure);
                    } else {
                        $this->updateTableSchema($table, $columnTypes, $structure);
                    }

                    $conn->statement("ALTER TABLE `{$table}` ROW_FORMAT=DYNAMIC");
                    Log::info('Estrutura da tabela processada', ['table' => $table]);
                }
            }

            // Processar dados, se fornecidos
            if (!empty($data['payload'])) {
                foreach ($data['payload'] as $rawTable => $rows) {
                    $table = $this->sanitizeTableName($rawTable);
                    $rowCount = is_array($rows) ? count($rows) : 0;
                    if ($rowCount === 0) {
                        Log::warning('Tabela vazia recebida', ['table' => $table]);
                        continue;
                    }

                    Log::info('Processando dados da tabela', ['table' => $table, 'rows' => $rowCount]);

                    $tableStructure = $data['structure'][$rawTable] ?? null;
                    $columnTypes = $tableStructure ? $this->mapStructureToTypes($tableStructure) : $this->inferColumnTypes($rows);

                    // Garantir que a tabela existe
                    if (!Schema::connection('tenant')->hasTable($table)) {
                        Log::info('Criando tabela para dados', ['table' => $table]);
                        $this->createTableWithTypes($table, $columnTypes, $tableStructure);
                        $conn->statement("ALTER TABLE `{$table}` ROW_FORMAT=DYNAMIC");
                    }

                    // Usar upsert para inserir ou atualizar registros
                    foreach (array_chunk($rows, 1000) as $batch) {
                        try {
                            $updateColumns = array_keys($batch[0]);
                            $updateColumns = array_diff($updateColumns, ['id']);
                            $conn->table($table)->upsert(
                                $batch,
                                ['id'],
                                $updateColumns
                            );
                            Log::info('Lote sincronizado via upsert', ['table' => $table, 'batch_size' => count($batch)]);
                            foreach ($batch as $row) {
                                $insertedRecords[$table][] = $row;
                            }
                        } catch (\Throwable $e) {
                            Log::error('Erro ao sincronizar lote', [
                                'table' => $table,
                                'error' => $e->getMessage(),
                                'batch_size' => count($batch)
                            ]);
                            throw $e;
                        }
                    }

                    Log::info('Dados sincronizados', ['table' => $table, 'count' => $rowCount]);
                }
            } else {
                Log::info('Nenhum dado fornecido, apenas estrutura processada');
            }

            $conn->commit();
            Log::info('Transação confirmada', ['database' => $client->database_name]);
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::info('Sincronização concluída com sucesso', [
                'tables' => $payloadCount,
                'inserted_records' => array_map(function ($records) {
                    return array_map(function ($record) {
                        return array_key_exists('id', $record) ? $record['id'] : 'no_id';
                    }, $records);
                }, $insertedRecords)
            ]);
            return response()->json([
                'message' => 'Sincronização completa',
                'tables' => $payloadCount,
                'inserted_records' => $insertedRecords
            ], 200);
        } catch (\Throwable $e) {
            if (isset($conn) && $conn->getPdo()->inTransaction()) {
                $conn->rollBack();
                Log::info('Transação revertida', ['database' => $client->database_name ?? 'unknown']);
            }
            if (isset($conn)) {
                $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            }
            Log::error('Erro na sincronização', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'payload' => $raw
            ]);
            return response()->json(['error' => 'Falha na sincronização', 'details' => $e->getMessage()], 500);
        }
    }

    public function verifyRecord(Request $request)
    {
        Log::info('Recebendo requisição de verificação de registro', [
            'ip' => $request->ip(),
            'payload' => $request->all(),
            'headers' => $request->headers->all()
        ]);

        try {
            $data = $request->validate([
                'table' => 'required|string',
                'record_id' => 'nullable',
                'unique_key' => 'nullable|array'
            ]);

            $client = Client::where('id', $request->user_identifier)
                ->orWhere('database_name', $request->user_identifier)
                ->first();
            if (!$client) {
                Log::error('Cliente não encontrado', ['user_identifier' => $request->user_identifier]);
                return response()->json(['error' => 'Cliente não encontrado', 'exists' => false], 404);
            }

            $this->connectToTenant($client);
            $conn = DB::connection('tenant');
            $table = $this->sanitizeTableName($data['table']);

            if (!Schema::connection('tenant')->hasTable($table)) {
                Log::error('Tabela não encontrada', ['table' => $table]);
                return response()->json(['error' => 'Tabela não encontrada', 'exists' => false], 404);
            }

            $columns = Schema::connection('tenant')->getColumnListing($table);
            Log::info('Colunas da tabela', ['table' => $table, 'columns' => $columns]);

            if (!empty($data['record_id']) && in_array('id', $columns)) {
                $exists = $conn->table($table)->where('id', $data['record_id'])->exists();
                Log::info('Verificação por ID', [
                    'table' => $table,
                    'record_id' => $data['record_id'],
                    'exists' => $exists
                ]);
                return response()->json(['exists' => $exists], 200);
            }

            if (!empty($data['unique_key']) && is_array($data['unique_key'])) {
                $query = $conn->table($table);
                foreach ($data['unique_key'] as $key => $value) {
                    if (in_array($key, $columns)) {
                        $query->where($key, $value);
                    } else {
                        Log::warning('Coluna não encontrada na tabela', ['table' => $table, 'column' => $key]);
                    }
                }
                $exists = $query->exists();
                Log::info('Verificação por chave única', [
                    'table' => $table,
                    'unique_key' => $data['unique_key'],
                    'exists' => $exists
                ]);
                return response()->json(['exists' => $exists], 200);
            }

            Log::warning('Nenhum critério de verificação válido fornecido', [
                'table' => $table,
                'record_id' => $data['record_id'] ?? null,
                'unique_key' => $data['unique_key'] ?? null
            ]);
            return response()->json(['error' => 'Nenhum critério de verificação válido', 'exists' => false], 400);
        } catch (\Illuminate\Validation\ValidationException $e) {
            Log::error('Erro de validação na verificação de registro', [
                'errors' => $e->errors(),
                'payload' => $request->all()
            ]);
            return response()->json(['error' => 'Validação inválida', 'details' => $e->errors()], 422);
        } catch (\Throwable $e) {
            Log::error('Erro ao verificar registro', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'payload' => $request->all()
            ]);
            return response()->json(['error' => 'Falha na verificação', 'details' => $e->getMessage()], 500);
        }
    }

    public function verifyTable($table, Request $request)
    {
        Log::info('Recebendo requisição de verificação de tabela', [
            'ip' => $request->ip(),
            'table' => $table,
            'user_identifier' => $request->user_identifier
        ]);

        try {
            $client = Client::where('id', $request->user_identifier)
                ->orWhere('database_name', $request->user_identifier)
                ->first();
            if (!$client) {
                Log::error('Cliente não encontrado', ['user_identifier' => $request->user_identifier]);
                return response()->json(['error' => 'Cliente não encontrado', 'count' => 0], 404);
            }

            $this->connectToTenant($client);
            $table = $this->sanitizeTableName($table);

            if (!Schema::connection('tenant')->hasTable($table)) {
                Log::error('Tabela não encontrada', ['table' => $table]);
                return response()->json(['error' => 'Tabela não encontrada', 'count' => 0], 404);
            }

            $count = DB::connection('tenant')->table($table)->count();
            $structure = $this->getTableStructure($table);

            Log::info('Verificação de tabela concluída', [
                'table' => $table,
                'count' => $count,
                'structure' => $structure
            ]);
            return response()->json([
                'count' => $count,
                'structure' => $structure
            ], 200);
        } catch (\Throwable $e) {
            Log::error('Erro ao verificar tabela', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            return response()->json(['error' => 'Falha na verificação', 'details' => $e->getMessage()], 500);
        }
    }

    protected function inferColumnTypes(array $rows): array
    {
        if (empty($rows)) {
            Log::warning('Nenhuma linha fornecida para inferir tipos de colunas');
            return [];
        }

        $columns = array_keys($rows[0]);
        $types = [];
        foreach ($columns as $col) {
            $maxInt = 0;
            $maxLen = 0;
            $isDate = false;
            foreach ($rows as $row) {
                $value = $row[$col] ?? null;
                if (is_numeric($value) && ctype_digit((string)$value)) {
                    $maxInt = max($maxInt, (int)$value);
                }
                if (is_string($value)) {
                    $maxLen = max($maxLen, mb_strlen($value));
                    if (preg_match('/^\d{4}-\d{2}-\d{2}(\s\d{2}:\d{2}:\d{2})?$/', $value)) {
                        $isDate = true;
                    }
                }
            }
            if ($isDate) {
                $types[$col] = 'datetime';
            } elseif ($maxInt > 2147483647) {
                $types[$col] = 'bigInteger';
            } elseif ($maxLen > 191) {
                $types[$col] = 'text';
            } else {
                $types[$col] = 'string';
            }
            $types[$col . '_nullable'] = true;
        }
        Log::info('Tipos de colunas inferidos', ['columns' => $types]);
        return $types;
    }

    protected function mapStructureToTypes(array $structure): array
    {
        $types = [];
        foreach ($structure['columns'] as $column) {
            $colName = $column['name'];
            $colType = strtolower($column['type']);
            $isNullable = $column['nullable'] ?? true;
            switch (true) {
                case str_contains($colType, 'bigint'):
                    $types[$colName] = 'bigInteger';
                    break;
                case str_contains($colType, 'varchar') || str_contains($colType, 'char'):
                    $types[$colName] = 'string';
                    break;
                case str_contains($colType, 'text'):
                    $types[$colName] = 'text';
                    break;
                case str_contains($colType, 'datetime') || str_contains($colType, 'timestamp'):
                    $types[$colName] = 'datetime';
                    break;
                case str_contains($colType, 'int'):
                    $types[$colName] = 'integer';
                    break;
                default:
                    $types[$colName] = 'string';
                    break;
            }
            $types[$colName . '_nullable'] = $isNullable;
        }
        Log::info('Tipos de colunas mapeados a partir da estrutura', ['columns' => $types]);
        return $types;
    }

    protected function createTableWithTypes(string $table, array $types, ?array $structure = null)
    {
        try {
            Schema::connection('tenant')->create($table, function (Blueprint $t) use ($types, $structure) {
                // Adicionar coluna 'id' apenas se não estiver na estrutura fornecida
                $hasIdColumn = array_key_exists('id', $types);
                if (!$hasIdColumn) {
                    $t->id()->nullable();
                }

                foreach ($types as $col => $type) {
                    if (str_ends_with($col, '_nullable')) {
                        continue;
                    }
                    $isNullable = $types[$col . '_nullable'] ?? true;
                    $column = null;
                    switch ($type) {
                        case 'bigInteger':
                            $column = $t->bigInteger($col);
                            break;
                        case 'text':
                            $column = $t->text($col);
                            break;
                        case 'datetime':
                            $column = $t->dateTime($col);
                            break;
                        case 'integer':
                            $column = $t->integer($col);
                            break;
                        default:
                            $column = $t->string($col, 191);
                            break;
                    }
                    if ($isNullable) {
                        $column->nullable();
                    }
                }
                $t->timestamps();
                $t->timestamp('synced_at')->nullable();

                // Adicionar índices, se fornecidos
                if ($structure && isset($structure['indexes'])) {
                    foreach ($structure['indexes'] as $index) {
                        if ($index['type'] === 'primary' && $index['column'] !== 'id') {
                            $t->primary($index['column']);
                        } elseif ($index['type'] === 'unique') {
                            $t->unique($index['column'], $index['name'] ?? null);
                        } elseif ($index['type'] === 'index') {
                            $t->index($index['column'], $index['name'] ?? null);
                        }
                    }
                }
            });
            Log::info('Tabela criada com sucesso', ['table' => $table, 'columns' => array_keys($types)]);
        } catch (\Throwable $e) {
            Log::error('Erro ao criar tabela', [
                'table' => $table,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw $e;
        }
    }

    protected function updateTableSchema(string $table, array $types, ?array $structure = null)
    {
        try {
            $existingCols = Schema::connection('tenant')->getColumnListing($table);
            Schema::connection('tenant')->table($table, function (Blueprint $t) use ($types, $existingCols, $structure) {
                foreach ($types as $col => $type) {
                    if (str_ends_with($col, '_nullable')) {
                        continue;
                    }
                    if (!in_array($col, $existingCols)) {
                        $isNullable = $types[$col . '_nullable'] ?? true;
                        $column = null;
                        switch ($type) {
                            case 'bigInteger':
                                $column = $t->bigInteger($col);
                                break;
                            case 'text':
                                $column = $t->text($col);
                                break;
                            case 'datetime':
                                $column = $t->dateTime($col);
                                break;
                            case 'integer':
                                $column = $t->integer($col);
                                break;
                            default:
                                $column = $t->string($col, 191);
                                break;
                        }
                        if ($isNullable) {
                            $column->nullable();
                        }
                    }
                }

                // Adicionar índices, se fornecidos
                if ($structure && isset($structure['indexes'])) {
                    foreach ($structure['indexes'] as $index) {
                        if ($index['type'] === 'unique') {
                            $t->unique($index['column'], $index['name'] ?? null);
                        } elseif ($index['type'] === 'index') {
                            $t->index($index['column'], $index['name'] ?? null);
                        }
                    }
                }
            });
            Log::info('Esquema da tabela atualizado', ['table' => $table, 'columns' => array_keys($types)]);
        } catch (\Throwable $e) {
            Log::error('Erro ao atualizar esquema da tabela', [
                'table' => $table,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw $e;
        }
    }

    protected function getTableStructure(string $table): array
    {
        try {
            $columns = Schema::connection('tenant')->getColumnListing($table);
            $columnDetails = [];
            $conn = DB::connection('tenant');
            $results = $conn->select("SHOW COLUMNS FROM `{$table}`");
            foreach ($results as $result) {
                $columnDetails[] = [
                    'name' => $result->Field,
                    'type' => $result->Type,
                    'nullable' => $result->Null === 'YES',
                    'default' => $result->Default,
                    'extra' => $result->Extra
                ];
            }

            $indexes = $conn->select("SHOW INDEXES FROM `{$table}`");
            $indexDetails = [];
            foreach ($indexes as $index) {
                $indexDetails[] = [
                    'name' => $index->Key_name,
                    'type' => $index->Key_name === 'PRIMARY' ? 'primary' : ($index->Non_unique ? 'index' : 'unique'),
                    'column' => $index->Column_name
                ];
            }

            return [
                'columns' => $columnDetails,
                'indexes' => $indexDetails
            ];
        } catch (\Throwable $e) {
            Log::error('Erro ao obter estrutura da tabela', [
                'table' => $table,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            return [];
        }
    }

    protected function sanitizeTableName(string $name): string
    {
        $sanitized = preg_replace('/[^a-zA-Z0-9_]/', '', $name);
        Log::info('Sanitização de nome de tabela', ['original' => $name, 'sanitized' => $sanitized]);
        return $sanitized;
    }

    protected function connectToTenant(Client $client)
    {
        try {
            // Configurar a conexão
            config(['database.connections.tenant' => [
                'driver' => 'mysql',
                'host' => env('DB_HOST', '127.0.0.1'),
                'port' => env('DB_PORT', '3306'),
                'database' => $client->database_name,
                'username' => env('DB_USERNAME'),
                'password' => env('DB_PASSWORD'),
                'charset' => 'utf8mb4',
                'collation' => 'utf8mb4_unicode_ci',
                'strict' => true,
            ]]);

            // Limpar qualquer conexão anterior
            DB::purge('tenant');
            DB::reconnect('tenant');

            // Testar a conexão
            $conn = DB::connection('tenant');
            $conn->getPdo(); // Forçar a conexão para verificar se está ativa
            Log::info('Conexão com tenant estabelecida', ['database' => $client->database_name]);
            return $conn;
        } catch (\Throwable $e) {
            Log::error('Erro ao conectar ao tenant', [
                'database' => $client->database_name,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            return null;
        }
    }
}
