<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\Client;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Schema;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Str;

class ReceiveDataController extends Controller
{
    /**
     * Store incoming data payload into the client-specific database.
     */
    public function store(Request $request)
    {
        Log::info('Requisição recebida no endpoint /receive', [
            'payload' => $request->all(),
            'headers' => $request->headers->all()
        ]);
        Log::info('Recebendo dados brutos:', ['dados' => $request->all()]);

        $data = $request->validate([
            'user_identifier' => 'required|string',
            'payload' => 'required|array',
        ]);

        Log::info('Requisição ReceiveData recebida', [
            'user_identifier' => $data['user_identifier'],
            'tabelas' => array_keys($data['payload']),
            'total_tabelas' => count($data['payload']),
            'total_linhas' => array_sum(array_map('count', $data['payload']))
        ]);

        try {
            $client = Client::where('id', $data['user_identifier'])
                ->orWhere('database_name', $data['user_identifier'])
                ->firstOrFail();
            Log::info('Cliente encontrado', ['client_id' => $client->id, 'database_name' => $client->database_name]);
        } catch (\Illuminate\Database\Eloquent\ModelNotFoundException $e) {
            Log::error('Cliente não encontrado para o identificador', ['user_identifier' => $data['user_identifier']]);
            return response()->json(['error' => 'Cliente não encontrado'], 404);
        }

        try {
            $this->connectToTenant($client);
            DB::connection('tenant')->getPdo();
            Log::info('Conectado com sucesso ao banco de dados do tenant', ['database' => $client->database_name]);
        } catch (\Exception $e) {
            Log::error('Falha ao conectar ao banco de dados do tenant', [
                'database' => $client->database_name,
                'erro' => $e->getMessage()
            ]);
            return response()->json(['error' => 'Falha na conexão com o banco de dados do tenant'], 500);
        }

        try {
            $connection = DB::connection('tenant');
            $connection->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, false);
            $connection->beginTransaction();
            Log::info('Transação iniciada para o tenant', ['database' => $client->database_name]);

            foreach ($data['payload'] as $table => $rows) {
                if (!is_array($rows) || empty($rows)) {
                    Log::info("Ignorando tabela vazia: $table");
                    continue;
                }

                Log::info('Iniciando processamento da tabela', ['tabela' => $table, 'linhas' => count($rows)]);
                $this->processTable($table, $rows);
                Log::info('Processamento da tabela concluído', ['tabela' => $table]);
            }

            $connection->commit();
            $connection->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::info('Todos os dados foram sincronizados com sucesso para o cliente', ['client_id' => $client->id]);

            return response()->json(['message' => 'Dados sincronizados com sucesso.']);
        } catch (\Throwable $e) {
            DB::connection('tenant')->rollBack();
            DB::connection('tenant')->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::error('Erro ao sincronizar dados', [
                'client_id' => $client->id ?? 'desconhecido',
                'erro' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);

            return response()->json([
                'error' => $e->getMessage(),
                'details' => 'Verifique os logs do servidor para mais informações'
            ], 500);
        }
    }

    /**
     * Process individual table
     */
    protected function processTable(string $table, array $rows)
    {
        if (!Schema::connection('tenant')->hasTable($table)) {
            Log::info('Tabela não existe, criando nova tabela', ['tabela' => $table]);
            $this->createTable($table, $rows[0]);
        } else {
            Log::info('Tabela já existe, verificando esquema', ['tabela' => $table]);
            $this->updateTableSchema($table, $rows[0]);
        }

        $columns = Schema::connection('tenant')->getColumnListing($table);
        $primaryKey = $this->guessPrimaryKey(array_keys($rows[0]), $table);
        $filteredRows = $this->filterRows($rows, $columns, $table, $primaryKey);

        $uniqueKey = $this->detectPrimaryKey($table, $columns, $rows[0]);

        // Logar os dados filtrados antes do upsert
        Log::debug('Dados filtrados para upsert', [
            'tabela' => $table,
            'uniqueKey' => $uniqueKey,
            'filteredRows' => $filteredRows
        ]);

        DB::connection('tenant')->table($table)->upsert(
            $filteredRows,
            is_array($uniqueKey) ? $uniqueKey : [$uniqueKey],
            array_diff($columns, array_merge((array)$uniqueKey, ['created_at', 'updated_at']))
        );

        Log::info('Upsert realizado na tabela', ['tabela' => $table, 'linhas' => count($filteredRows)]);
    }

    /**
     * Create new table with dynamic schema
     */
    protected function createTable(string $table, array $firstRow)
    {
        Log::info('Criando nova tabela', ['tabela' => $table]);

        $primaryKey = $this->guessPrimaryKey(array_keys($firstRow), $table);
        $columns = array_keys($firstRow);

        Schema::connection('tenant')->create($table, function (Blueprint $t) use ($firstRow, $primaryKey, $columns) {
            if ($primaryKey && in_array($primaryKey, $columns)) {
                $t->unsignedBigInteger('id')->primary();
            } else {
                $t->id();
            }

            foreach ($columns as $column) {
                if ($column === $primaryKey || strtolower($column) === 'id') {
                    continue;
                }

                $value = $firstRow[$column];
                if (is_int($value)) {
                    $t->integer($column)->nullable();
                } elseif (is_float($value)) {
                    $t->float($column)->nullable();
                } elseif ($this->isDateTime($value)) {
                    $t->dateTime($column)->nullable();
                } else {
                    $t->text($column)->nullable();
                }
            }

            $t->timestamps();
        });

        Log::info('Nova tabela criada', ['tabela' => $table, 'colunas' => array_keys($firstRow), 'chave_primaria' => $primaryKey ?: 'id']);
    }

    /**
     * Update existing table schema to add new columns
     */
    protected function updateTableSchema(string $table, array $firstRow)
    {
        $existingColumns = Schema::connection('tenant')->getColumnListing($table);
        $primaryKey = $this->guessPrimaryKey(array_keys($firstRow), $table);
        $newColumns = array_diff(array_keys($firstRow), $existingColumns);

        // Ignorar 'ID' se 'id' já existe como chave primária
        if ($primaryKey && in_array($primaryKey, $newColumns) && in_array('id', $existingColumns)) {
            $newColumns = array_diff($newColumns, [$primaryKey]);
        }

        if (empty($newColumns)) {
            Log::info('Nenhuma nova coluna detectada para a tabela', ['tabela' => $table]);
            return;
        }

        Log::info('Novas colunas detectadas, atualizando esquema', [
            'tabela' => $table,
            'novas_colunas' => $newColumns
        ]);

        Schema::connection('tenant')->table($table, function (Blueprint $t) use ($firstRow, $newColumns) {
            foreach ($newColumns as $column) {
                if (strtolower($column) === 'id') {
                    continue;
                }

                $value = $firstRow[$column];
                if (is_int($value)) {
                    $t->integer($column)->nullable()->after('id');
                } elseif (is_float($value)) {
                    $t->float($column)->nullable()->after('id');
                } elseif ($this->isDateTime($value)) {
                    $t->dateTime($column)->nullable()->after('id');
                } else {
                    $t->text($column)->nullable()->after('id');
                }
            }
        });

        Log::info('Esquema da tabela atualizado', ['tabela' => $table, 'colunas_adicionadas' => $newColumns]);
    }

    /**
     * Filter rows to match table columns
     */
    protected function filterRows(array $rows, array $columns, string $table, ?string $primaryKey): array
    {
        $mappedRows = array_map(function ($row) use ($columns, $primaryKey, $table) {
            $mappedRow = [];
            foreach ($row as $key => $value) {
                // Mapear a chave primária do payload para 'id' se a coluna 'id' existe na tabela
                $mappedKey = ($primaryKey && $key === $primaryKey && in_array('id', $columns)) ? 'id' : $key;
                if (in_array($mappedKey, $columns)) {
                    $mappedRow[$mappedKey] = $value;
                }
            }
            return $mappedRow;
        }, $rows);

        // Verificar colunas extras após o mapeamento
        $payloadColumns = array_keys($rows[0]);
        // Se a chave primária existe e está mapeada para 'id', removê-la das colunas do payload
        if ($primaryKey && in_array('id', $columns) && in_array($primaryKey, $payloadColumns)) {
            $payloadColumns = array_diff($payloadColumns, [$primaryKey]);
        }
        $extraColumns = array_diff($payloadColumns, $columns);

        if (!empty($extraColumns)) {
            Log::warning('Colunas no payload não presentes na tabela', [
                'tabela' => $table,
                'colunas_extras' => $extraColumns
            ]);
        }

        return $mappedRows;
    }

    /**
     * Detect primary key with schema inspection or heuristic
     */
    protected function detectPrimaryKey(string $table, array $columns, array $firstRow): array
    {
        $primaryKey = $this->guessPrimaryKey(array_keys($firstRow), $table);
        if ($primaryKey && in_array('id', $columns)) {
            Log::info('Chave primária mapeada', ['tabela' => $table, 'chave' => $primaryKey, 'mapeada_para' => 'id']);
            return ['id'];
        }

        try {
            $schemaManager = Schema::connection('tenant')->getConnection()->getDoctrineConnection()->getSchemaManager();
            $tableDetails = $schemaManager->listTableDetails($table);

            $primaryKeySchema = $tableDetails->getPrimaryKey();
            if ($primaryKeySchema) {
                $pkColumns = $primaryKeySchema->getColumns();
                Log::info('Chave primária detectada via schema', ['tabela' => $table, 'chave' => $pkColumns]);
                return $pkColumns;
            }
        } catch (\Throwable $e) {
            Log::warning('Falha ao detectar chave primária via schema', [
                'tabela' => $table,
                'erro' => $e->getMessage()
            ]);
        }

        $fallbackKey = $columns[0] ?? null;
        if ($fallbackKey) {
            Log::warning('Usando fallback para chave primária', ['tabela' => $table, 'chave' => $fallbackKey]);
            return [$fallbackKey];
        } else {
            Log::error('Não foi possível determinar uma chave única para a tabela', ['tabela' => $table]);
            throw new \Exception('Não foi possível determinar uma chave única para a tabela ' . $table);
        }
    }

    /**
     * Guess the primary key based on table name and column names
     */
    protected function guessPrimaryKey(array $columns, string $table): ?string
    {
        $tablePrimaryKeyMap = [
            'wp_users' => 'ID',
            'wp_comments' => 'comment_ID',
            'wp_postmeta' => 'meta_id',
            'wp_usermeta' => 'umeta_id',
            'wp_terms' => 'term_id',
            'wp_term_taxonomy' => 'term_taxonomy_id',
            'wp_term_relationships' => ['object_id', 'term_taxonomy_id'],
            'wp_posts' => 'ID',
        ];

        if (isset($tablePrimaryKeyMap[$table])) {
            $primaryKey = $tablePrimaryKeyMap[$table];
            if (is_array($primaryKey)) {
                if (count(array_intersect($primaryKey, $columns)) === count($primaryKey)) {
                    return $primaryKey[0];
                }
            } elseif (in_array($primaryKey, $columns)) {
                return $primaryKey;
            }
        }

        foreach ($columns as $column) {
            if (in_array(strtolower($column), ['id', $table . '_id'])) {
                return $column;
            }
        }

        return null;
    }

    /**
     * Check if value is datetime
     */
    protected function isDateTime($value): bool
    {
        if (!$value || !is_string($value)) return false;

        try {
            new \DateTime($value);
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * Configure tenant database connection
     */
    protected function connectToTenant(Client $client)
    {
        $dbName = $client->database_name;

        try {
            config([
                'database.connections.tenant' => [
                    'driver' => 'mysql',
                    'host' => env('DB_HOST', '127.0.0.1'),
                    'port' => env('DB_PORT', '3306'),
                    'database' => $dbName,
                    'username' => env('DB_USERNAME', 'your_username'),
                    'password' => env('DB_PASSWORD', 'your_password'),
                    'charset' => 'utf8mb4',
                    'collation' => 'utf8mb4_unicode_ci',
                    'prefix' => '',
                    'strict' => true,
                    'engine' => null,
                    'options' => [
                        \PDO::ATTR_AUTOCOMMIT => false,
                    ],
                ]
            ]);

            DB::purge('tenant');
            DB::reconnect('tenant');
            DB::connection('tenant')->getPdo();
            Log::info('Conexão bem-sucedida com MySQL', ['database' => $dbName]);
            return;
        } catch (\Exception $e) {
            Log::warning('Falha ao conectar com MySQL, tentando SQLite', [
                'database' => $dbName,
                'erro' => $e->getMessage()
            ]);
        }

        $sqlitePath = database_path("tenants/{$dbName}.sqlite");
        if (!file_exists($sqlitePath)) {
            Log::warning('Arquivo SQLite não encontrado, pulando tentativa de conexão', ['path' => $sqlitePath]);
            throw new \Exception('Não foi possível conectar ao banco de dados do tenant: ' . $dbName);
        }

        try {
            config([
                'database.connections.tenant' => [
                    'driver' => 'sqlite',
                    'database' => $sqlitePath,
                    'prefix' => '',
                    'foreign_key_constraints' => true,
                ]
            ]);

            DB::purge('tenant');
            DB::reconnect('tenant');
            DB::connection('tenant')->getPdo();
            Log::info('Conexão bem-sucedida com SQLite', ['database' => $sqlitePath]);
            return;
        } catch (\Exception $e) {
            Log::error('Falha ao conectar com SQLite', [
                'database' => $sqlitePath,
                'erro' => $e->getMessage()
            ]);
            throw new \Exception('Não foi possível conectar ao banco de dados do tenant: ' . $dbName);
        }
    }
}
