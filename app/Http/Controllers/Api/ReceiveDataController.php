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
            'payload' => 'required|array|min:1',
            'payload.*' => 'array|min:1',
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
            $this->connectToTenant($client);

            $conn = DB::connection('tenant');
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, false);
            $conn->beginTransaction();

            $insertedIds = []; // Para rastrear IDs inseridos

            foreach ($data['payload'] as $rawTable => $rows) {
                $table = $this->sanitizeTableName($rawTable);
                $rowCount = is_array($rows) ? count($rows) : 0;
                if ($rowCount === 0) {
                    Log::warning('Tabela vazia recebida', ['table' => $table]);
                    continue;
                }

                Log::info('Processando tabela', ['table' => $table, 'rows' => $rowCount]);

                // Inferir tipos de colunas
                $columnTypes = $this->inferColumnTypes($rows);

                // Verificar e criar/recriar tabela
                if (Schema::connection('tenant')->hasTable($table)) {
                    $existingCols = Schema::connection('tenant')->getColumnListing($table);
                    $newCols = array_keys($columnTypes);
                    sort($existingCols);
                    sort($newCols);

                    if ($existingCols !== $newCols) {
                        Log::info('Recriando tabela devido a colunas diferentes', [
                            'table' => $table,
                            'existing_columns' => $existingCols,
                            'new_columns' => $newCols
                        ]);
                        Schema::connection('tenant')->dropIfExists($table);
                        $this->createTableWithTypes($table, $columnTypes);
                    }
                } else {
                    Log::info('Criando nova tabela', ['table' => $table]);
                    $this->createTableWithTypes($table, $columnTypes);
                }

                // Configurar formato da tabela
                $conn->statement("ALTER TABLE `{$table}` ROW_FORMAT=DYNAMIC");
                $conn->table($table)->truncate();

                // Inserir dados em lotes
                foreach (array_chunk($rows, 1000) as $batch) {
                    try {
                        $conn->table($table)->insert($batch);
                        Log::info('Lote inserido', ['table' => $table, 'batch_size' => count($batch)]);
                        // Rastrear IDs inseridos (se aplicável)
                        foreach ($batch as $row) {
                            if (isset($row['id'])) {
                                $insertedIds[$table][] = $row['id'];
                            }
                        }
                    } catch (\Throwable $e) {
                        Log::error('Erro ao inserir lote', [
                            'table' => $table,
                            'error' => $e->getMessage(),
                            'batch_size' => count($batch)
                        ]);
                        throw $e;
                    }
                }

                Log::info('Dados gravados', ['table' => $table, 'count' => $rowCount]);
            }

            $conn->commit();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::info('Sincronização concluída com sucesso', [
                'tables' => $payloadCount,
                'inserted_ids' => $insertedIds
            ]);
            return response()->json([
                'message' => 'Sincronização completa',
                'tables' => $payloadCount,
                'inserted_ids' => $insertedIds
            ], 200);
        } catch (\Throwable $e) {
            $conn->rollBack();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
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
            // Validar entrada
            $data = $request->validate([
                'table' => 'required|string',
                'record_id' => 'required'
            ]);

            // Verificar cliente
            $client = Client::where('id', $request->user_identifier)
                ->orWhere('database_name', $request->user_identifier)
                ->first();
            if (!$client) {
                Log::error('Cliente não encontrado', ['user_identifier' => $request->user_identifier]);
                return response()->json(['error' => 'Cliente não encontrado', 'exists' => false], 404);
            }

            // Conectar ao tenant
            $this->connectToTenant($client);
            $conn = DB::connection('tenant');
            $table = $this->sanitizeTableName($data['table']);

            // Verificar se a tabela existe
            if (!Schema::connection('tenant')->hasTable($table)) {
                Log::error('Tabela não encontrada', ['table' => $table]);
                return response()->json(['error' => 'Tabela não encontrada', 'exists' => false], 404);
            }

            // Verificar se a coluna 'id' existe
            $columns = Schema::connection('tenant')->getColumnListing($table);
            if (!in_array('id', $columns)) {
                Log::error('Coluna "id" não encontrada na tabela', ['table' => $table, 'columns' => $columns]);
                return response()->json(['error' => 'Coluna "id" não encontrada na tabela', 'exists' => false], 400);
            }

            // Verificar se o registro existe
            $exists = $conn->table($table)->where('id', $data['record_id'])->exists();

            Log::info('Resultado da verificação de registro', [
                'table' => $table,
                'record_id' => $data['record_id'],
                'exists' => $exists
            ]);

            return response()->json(['exists' => $exists], 200);
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
                return response()->json(['error' => 'Cliente não encontrado'], 404);
            }

            $this->connectToTenant($client);
            $table = $this->sanitizeTableName($table);

            if (!Schema::connection('tenant')->hasTable($table)) {
                Log::error('Tabela não encontrada', ['table' => $table]);
                return response()->json(['error' => 'Tabela não encontrada', 'count' => 0], 404);
            }

            $count = DB::connection('tenant')->table($table)->count();

            Log::info('Verificação de tabela', ['table' => $table, 'count' => $count]);
            return response()->json(['count' => $count], 200);
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
            foreach ($rows as $row) {
                $value = $row[$col] ?? null;
                if (is_numeric($value) && ctype_digit((string) $value)) {
                    $maxInt = max($maxInt, (int) $value);
                }
                if (is_string($value)) {
                    $maxLen = max($maxLen, mb_strlen($value));
                }
            }
            if ($maxInt > 2147483647) {
                $types[$col] = 'bigInteger';
            } elseif ($maxLen > 191) {
                $types[$col] = 'text';
            } else {
                $types[$col] = 'string';
            }
        }
        Log::info('Tipos de colunas inferidos', ['columns' => $types]);
        return $types;
    }

    protected function createTableWithTypes(string $table, array $types)
    {
        try {
            Schema::connection('tenant')->create($table, function (Blueprint $t) use ($types) {
                $t->id();
                foreach ($types as $col => $type) {
                    switch ($type) {
                        case 'bigInteger':
                            $t->bigInteger($col)->nullable();
                            break;
                        case 'text':
                            $t->text($col)->nullable();
                            break;
                        default:
                            $t->string($col, 191)->nullable();
                            break;
                    }
                }
                $t->timestamps();
                $t->timestamp('synced_at')->nullable();
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

    protected function sanitizeTableName(string $name): string
    {
        $sanitized = preg_replace('/[^a-zA-Z0-9_]/', '', $name);
        Log::info('Sanitização de nome de tabela', ['original' => $name, 'sanitized' => $sanitized]);
        return $sanitized;
    }

    protected function connectToTenant(Client $client)
    {
        try {
            config(['database.connections.tenant' => [
                'driver'    => 'mysql',
                'host'      => env('DB_HOST', '127.0.0.1'),
                'port'      => env('DB_PORT', '3306'),
                'database'  => $client->database_name,
                'username'  => env('DB_USERNAME'),
                'password'  => env('DB_PASSWORD'),
                'charset'   => 'utf8mb4',
                'collation' => 'utf8mb4_unicode_ci',
                'strict'    => true,
            ]]);
            DB::purge('tenant');
            DB::reconnect('tenant');
            Log::info('Conexão com tenant estabelecida', ['database' => $client->database_name]);
        } catch (\Throwable $e) {
            Log::error('Erro ao conectar ao tenant', [
                'database' => $client->database_name,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw $e;
        }
    }
}
