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

        $data = $request->validate([
            'user_identifier' => 'required|string',
            'payload' => 'required|array',
            'payload.*' => 'array',
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
            $conn = DB::connection('tenant');
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, false);
            $conn->beginTransaction();
            Log::info('Transação iniciada para o tenant', ['database' => $client->database_name]);

            foreach ($data['payload'] as $rawTable => $rows) {
                // Sanitizar nome de tabela
                $table = $this->sanitizeTableName($rawTable);
                if (!is_array($rows) || empty($rows)) {
                    Log::info("Ignorando tabela vazia: $table");
                    continue;
                }

                Log::info('Iniciando processamento da tabela', ['tabela' => $table, 'linhas' => count($rows)]);
                $this->processTable($table, $rows);
                Log::info('Processamento da tabela concluído', ['tabela' => $table]);
            }

            $conn->commit();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
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
        $primaryKey = $this->guessPrimaryKey($columns, $table);
        $filteredRows = $this->filterRows($rows, $columns, $table, $primaryKey);

        $uniqueKey = $this->detectPrimaryKey($table, $columns, $rows[0]);
        Log::debug('Dados filtrados para upsert', ['tabela' => $table, 'uniqueKey' => $uniqueKey, 'filteredRows' => $filteredRows]);

        DB::connection('tenant')->table($table)->upsert(
            $filteredRows,
            is_array($uniqueKey) ? $uniqueKey : [$uniqueKey],
            array_diff(
                $columns,
                array_merge((array)$uniqueKey, ['created_at', 'updated_at', 'synced_at'])
            )
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
            $t->id();
            foreach ($columns as $column) {
                if ($column === $primaryKey || Str::lower($column) === 'id') continue;

                $value = $firstRow[$column];
                if ($this->isBoolean($value)) {
                    $t->boolean($column)->nullable();
                } elseif ($this->isDate($value)) {
                    $t->date($column)->nullable();
                } elseif ($this->isDateTime($value)) {
                    $t->dateTime($column)->nullable();
                } elseif (is_int($value)) {
                    $t->integer($column)->nullable();
                } elseif (is_float($value)) {
                    $t->float($column)->nullable();
                } else {
                    $t->string($column, 255)->nullable();
                }
            }
            $t->timestamps();
            $t->timestamp('synced_at')->nullable();
        });

        Log::info('Nova tabela criada', ['tabela' => $table]);
    }

    /**
     * Update existing table schema to add new columns
     */
    protected function updateTableSchema(string $table, array $firstRow)
    {
        $existing = Schema::connection('tenant')->getColumnListing($table);
        $newCols = array_diff(array_keys($firstRow), $existing);
        if (empty($newCols)) {
            Log::info('Nenhuma nova coluna detectada para a tabela', ['tabela' => $table]);
            return;
        }
        Log::info('Novas colunas detectadas, atualizando esquema', ['tabela' => $table, 'novas_colunas' => $newCols]);

        Schema::connection('tenant')->table($table, function (Blueprint $t) use ($firstRow, $newCols) {
            foreach ($newCols as $column) {
                if (Str::lower($column) === 'id') continue;

                $value = $firstRow[$column];
                if ($this->isBoolean($value)) {
                    $t->boolean($column)->nullable()->after('id');
                } elseif ($this->isDate($value)) {
                    $t->date($column)->nullable()->after('id');
                } elseif ($this->isDateTime($value)) {
                    $t->dateTime($column)->nullable()->after('id');
                } elseif (is_int($value)) {
                    $t->integer($column)->nullable()->after('id');
                } elseif (is_float($value)) {
                    $t->float($column)->nullable()->after('id');
                } else {
                    $t->string($column, 255)->nullable()->after('id');
                }
            }
        });

        Log::info('Esquema da tabela atualizado', ['tabela' => $table]);
    }

    /**
     * Filter rows to match table columns
     */
    protected function filterRows(array $rows, array $columns, string $table, ?string $primaryKey): array
    {
        $mapped = array_map(function ($row) use ($columns, $primaryKey) {
            $out = [];
            foreach ($row as $k => $v) {
                $key = ($primaryKey && $k === $primaryKey && in_array('id', $columns)) ? 'id' : $k;
                if (in_array($key, $columns)) {
                    $out[$key] = $v;
                }
            }
            return $out;
        }, $rows);

        $payloadCols = array_keys($rows[0]);
        if ($primaryKey && in_array('id', $columns) && in_array($primaryKey, $payloadCols)) {
            $payloadCols = array_diff($payloadCols, [$primaryKey]);
        }
        $extra = array_diff($payloadCols, $columns);
        if (!empty($extra)) {
            Log::warning('Colunas no payload não presentes na tabela', ['tabela' => $table, 'colunas_extras' => $extra]);
        }

        return $mapped;
    }

    /**
     * Detect primary key with schema inspection or heuristic
     */
    protected function detectPrimaryKey(string $table, array $columns, array $firstRow): array
    {
        // heurística inicial
        $pk = $this->guessPrimaryKey($columns, $table);
        if ($pk && in_array('id', $columns)) {
            return ['id'];
        }

        try {
            $manager = Schema::connection('tenant')->getConnection()->getDoctrineConnection()->getSchemaManager();
            $details = $manager->listTableDetails($table);
            $schemaPk = $details->getPrimaryKey();
            if ($schemaPk) {
                return $schemaPk->getColumns();
            }
        } catch (\Throwable $e) {
            Log::warning('Falha ao detectar chave primária via schema', ['tabela' => $table, 'erro' => $e->getMessage()]);
        }

        $fallback = $columns[0] ?? null;
        if ($fallback) {
            Log::warning('Usando fallback para chave primária', ['tabela' => $table, 'chave' => $fallback]);
            return [$fallback];
        }

        throw new \Exception("Não foi possível determinar chave única para tabela $table");
    }

    /**
     * Guess the primary key based on heuristics
     */
    protected function guessPrimaryKey(array $columns, string $table): ?string
    {
        $map = [
            'wp_users' => 'ID', 'wp_comments' => 'comment_ID', 'wp_postmeta' => 'meta_id',
            'wp_usermeta' => 'umeta_id', 'wp_terms' => 'term_id',
            'wp_term_taxonomy' => 'term_taxonomy_id', 'wp_posts' => 'ID',
        ];
        if (isset($map[$table])) {
            return is_array($map[$table]) ? $map[$table][0] : $map[$table];
        }

        foreach ($columns as $col) {
            if (in_array(Str::lower($col), ['id', $table . '_id'])) {
                return $col;
            }
        }

        return null;
    }

    /**
     * Check if value is datetime
     */
    protected function isDateTime($value): bool
    {
        return is_string($value) && strtotime($value) !== false && preg_match('/\d{2}:\d{2}:\d{2}/', $value);
    }

    /**
     * Check if value is date only
     */
    protected function isDate($value): bool
    {
        return is_string($value) && strtotime($value) !== false && !preg_match('/\d{2}:\d{2}:\d{2}/', $value);
    }

    /**
     * Check if value is boolean
     */
    protected function isBoolean($value): bool
    {
        return in_array($value, [0, 1, '0', '1', true, false], true);
    }

    /**
     * Sanitize table name to prevent SQL injection or invalid names
     */
    protected function sanitizeTableName(string $name): string
    {
        return preg_replace('/[^a-zA-Z0-9_]/', '', $name);
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
                    'username' => env('DB_USERNAME'),
                    'password' => env('DB_PASSWORD'),
                    'charset' => 'utf8mb4',
                    'collation' => 'utf8mb4_unicode_ci',
                    'prefix' => '',
                    'strict' => true,
                    'engine' => null,
                ]
            ]);

            DB::purge('tenant');
            DB::reconnect('tenant');
            DB::connection('tenant')->getPdo();
            Log::info('Conexão bem-sucedida com MySQL', ['database' => $dbName]);
            return;
        } catch (\Exception $e) {
            Log::warning('Falha ao conectar com MySQL, tentando SQLite', ['database' => $dbName, 'erro' => $e->getMessage()]);
        }

        $sqlitePath = database_path("tenants/{$dbName}.sqlite");
        if (!file_exists($sqlitePath)) {
            Log::warning('Arquivo SQLite não encontrado, pulando tentativa', ['path' => $sqlitePath]);
            throw new \Exception('Não foi possível conectar ao banco de dados do tenant: ' . $dbName);
        }

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
    }

    /**
     * Provide connection details and metadata for BI tools.
     */
    public function connectBI(Request $request)
    {
        Log::info('Requisição recebida no endpoint /connectbi', [
            'payload' => $request->all(),
            'headers' => $request->headers->all()
        ]);

        // Validação dos dados de entrada
        $data = $request->validate([
            'user_identifier' => 'required|string',
            'database_name' => 'nullable|string', // Opcional, caso queira especificar o banco diretamente
        ]);

        try {
            // Buscar o cliente pelo identificador
            $client = Client::where('id', $data['user_identifier'])
                ->orWhere('database_name', $data['user_identifier'])
                ->firstOrFail();
            Log::info('Cliente encontrado', ['client_id' => $client->id, 'database_name' => $client->database_name]);
        } catch (\Illuminate\Database\Eloquent\ModelNotFoundException $e) {
            Log::error('Cliente não encontrado para o identificador', ['user_identifier' => $data['user_identifier']]);
            return response()->json(['error' => 'Cliente não encontrado'], 404);
        }

        try {
            // Conectar ao banco do cliente
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
            // Obter informações do banco
            $connectionDetails = [
                'driver' => config('database.connections.tenant.driver'),
                'host' => config('database.connections.tenant.host', '127.0.0.1'),
                'port' => config('database.connections.tenant.port', '3306'),
                'database' => $client->database_name,
                'charset' => config('database.connections.tenant.charset', 'utf8mb4'),
                'collation' => config('database.connections.tenant.collation', 'utf8mb4_unicode_ci'),
            ];

            // Opcional: Listar tabelas disponíveis no banco
            $tables = Schema::connection('tenant')->getAllTables();
            $tableNames = array_map(function ($table) use ($connectionDetails) {
                $tableName = is_object($table) ? array_values((array)$table)[0] : $table;
                return [
                    'table_name' => $tableName,
                    // Opcional: Listar colunas da tabela
                    'columns' => Schema::connection('tenant')->getColumnListing($tableName),
                ];
            }, $tables);

            Log::info('Informações do banco preparadas para o BI', [
                'client_id' => $client->id,
                'database' => $client->database_name,
                'tables_count' => count($tableNames)
            ]);

            // Retornar detalhes de conexão e metadados
            return response()->json([
                'message' => 'Informações de conexão para o BI obtidas com sucesso.',
                'connection' => $connectionDetails,
                'tables' => $tableNames,
            ]);
        } catch (\Throwable $e) {
            Log::error('Erro ao obter informações do banco para o BI', [
                'client_id' => $client->id ?? 'desconhecido',
                'erro' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);

            return response()->json([
                'error' => 'Erro ao obter informações do banco',
                'details' => 'Verifique os logs do servidor para mais informações'
            ], 500);
        }
    }
}
