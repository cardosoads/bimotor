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
        Log::info('Requisição /receive', ['payload' => $request->all(), 'headers' => $request->headers->all()]);

        $data = $request->validate([
            'user_identifier' => 'required|string',
            'payload'         => 'required|array',
            'payload.*'       => 'array',
        ]);

        Log::info('Payload recebido', [
            'user_identifier' => $data['user_identifier'],
            'tables'          => array_keys($data['payload']),
            'total_tables'    => count($data['payload']),
            'total_rows'      => array_sum(array_map('count', $data['payload'])),
        ]);

        // Find client and connect tenant DB
        try {
            $client = Client::where('id', $data['user_identifier'])
                ->orWhere('database_name', $data['user_identifier'])
                ->firstOrFail();
        } catch (\Exception $e) {
            Log::error('Cliente não encontrado', ['identifier' => $data['user_identifier']]);
            return response()->json(['error' => 'Cliente não encontrado'], 404);
        }

        try {
            $this->connectToTenant($client);
            DB::connection('tenant')->getPdo();
        } catch (\Exception $e) {
            Log::error('Falha conexão tenant', ['error' => $e->getMessage()]);
            return response()->json(['error' => 'Falha na conexão com o tenant'], 500);
        }

        $conn = DB::connection('tenant');
        $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, false);
        $conn->beginTransaction();

        try {
            foreach ($data['payload'] as $rawTable => $rows) {
                $table = $this->sanitizeTableName($rawTable);
                if (empty($rows)) {
                    Log::info("Ignorando tabela vazia: $table");
                    continue;
                }
                Log::info('Processando tabela', ['table' => $table, 'rows' => count($rows)]);

                // Infer column types based on ALL rows
                $columnTypes = $this->inferColumnTypes($rows);

                if (!Schema::connection('tenant')->hasTable($table)) {
                    $this->createTableWithTypes($table, $columnTypes);
                } else {
                    $this->updateTableSchemaWithTypes($table, $columnTypes);
                }

                // Avoid row-size errors
                DB::connection('tenant')->statement("ALTER TABLE `$table` ROW_FORMAT=DYNAMIC");

                $cols   = Schema::connection('tenant')->getColumnListing($table);
                $pk     = $this->guessPrimaryKey($cols, $table);
                $filtered = $this->filterRows($rows, $cols, $pk);
                $unique   = $this->detectPrimaryKey($table, $cols);
                $updateCols = array_diff($cols, array_merge((array) $unique, ['created_at','updated_at','synced_at']));

                // Resilient upsert in batches
                foreach (array_chunk($filtered, 500) as $batch) {
                    try {
                        DB::connection('tenant')->table($table)->upsert($batch, (array)$unique, $updateCols);
                        Log::info('Batch upsert com sucesso', ['table' => $table, 'count' => count($batch)]);
                    } catch (\Exception $e) {
                        Log::warning('Erro no batch upsert, tentando linha a linha', ['table' => $table, 'error' => $e->getMessage()]);
                        foreach ($batch as $row) {
                            try {
                                DB::connection('tenant')->table($table)->upsert([$row], (array)$unique, $updateCols);
                            } catch (\Exception $ex) {
                                Log::error('Linha problemática', ['table' => $table, 'row' => $row, 'error' => $ex->getMessage()]);
                            }
                        }
                    }
                }
            }

            $conn->commit();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::info('Sincronização concluída', ['client_id' => $client->id]);

            return response()->json(['message' => 'Dados sincronizados com sucesso']);
        } catch (\Throwable $e) {
            $conn->rollBack();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::error('Erro na transação', ['error' => $e->getMessage(), 'trace' => $e->getTraceAsString()]);
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }

    /**
     * Infer column types from all rows
     */
    protected function inferColumnTypes(array $rows): array
    {
        $columns = array_keys($rows[0]);
        $types = [];
        foreach ($columns as $col) {
            $maxInt = 0;
            $maxLen = 0;
            foreach ($rows as $row) {
                if (!array_key_exists($col, $row)) continue;
                $v = $row[$col];
                if (is_numeric($v) && ctype_digit((string)$v)) {
                    $maxInt = max($maxInt, (int)$v);
                }
                if (is_string($v)) {
                    $maxLen = max($maxLen, mb_strlen($v));
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
        return $types;
    }

    /**
     * Create table based on column types map
     */
    protected function createTableWithTypes(string $table, array $types)
    {
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
                    case 'string':
                    default:
                        $t->string($col, 191)->nullable();
                        break;
                }
            }
            $t->timestamps();
            $t->timestamp('synced_at')->nullable();
        });
    }

    /**
     * Update existing schema by adding new cols with correct types
     */
    protected function updateTableSchemaWithTypes(string $table, array $types)
    {
        $existing = Schema::connection('tenant')->getColumnListing($table);
        $new = array_diff(array_keys($types), $existing);
        if (empty($new)) return;

        Schema::connection('tenant')->table($table, function (Blueprint $t) use ($types, $new) {
            foreach ($new as $col) {
                switch ($types[$col]) {
                    case 'bigInteger':
                        $t->bigInteger($col)->nullable()->after('id');
                        break;
                    case 'text':
                        $t->text($col)->nullable()->after('id');
                        break;
                    case 'string':
                    default:
                        $t->string($col, 191)->nullable()->after('id');
                        break;
                }
            }
        });
    }

    /**
     * Filter rows to only existing columns
     */
    protected function filterRows(array $rows, array $cols, ?string $pk): array
    {
        return array_map(function ($row) use ($cols, $pk) {
            $out = [];
            foreach ($row as $k => $v) {
                $key = ($pk && $k === $pk && in_array('id', $cols)) ? 'id' : $k;
                if (in_array($key, $cols)) {
                    $out[$key] = $v;
                }
            }
            return $out;
        }, $rows);
    }

    /**
     * Detect primary key (id or first column)
     */
    protected function detectPrimaryKey(string $table, array $cols): array
    {
        if (in_array('id', $cols)) {
            return ['id'];
        }
        try {
            $sm = Schema::connection('tenant')->getConnection()->getDoctrineConnection()->getSchemaManager();
            $pk = $sm->listTableDetails($table)->getPrimaryKey();
            if ($pk) {
                return $pk->getColumns();
            }
        } catch (\Throwable $e) {
        }
        return [array_key_first($cols)];
    }

    /**
     * Guess primary key by naming convention
     */
    protected function guessPrimaryKey(array $cols, string $table): ?string
    {
        foreach ($cols as $col) {
            if (Str::lower($col) === 'id' || Str::lower($col) === $table . '_id') {
                return $col;
            }
        }
        return null;
    }

    protected function sanitizeTableName(string $name): string
    {
        return preg_replace('/[^a-zA-Z0-9_]/', '', $name);
    }

    /**
     * Configure tenant connection with MySQL fallback to SQLite.
     */
    protected function connectToTenant(Client $client)
    {
        $db = $client->database_name;
        // MySQL config
        config(['database.connections.tenant' => [
            'driver'    => 'mysql',
            'host'      => env('DB_HOST', '127.0.0.1'),
            'port'      => env('DB_PORT', '3306'),
            'database'  => $db,
            'username'  => env('DB_USERNAME'),
            'password'  => env('DB_PASSWORD'),
            'charset'   => 'utf8mb4',
            'collation' => 'utf8mb4_unicode_ci',
            'strict'    => true,
        ]]);
        DB::purge('tenant');
        DB::reconnect('tenant');

        try {
            DB::connection('tenant')->getPdo();
            return;
        } catch (\Exception $e) {
            Log::warning('MySQL falhou, tentar SQLite', ['db' => $db, 'error' => $e->getMessage()]);
        }

        // SQLite fallback
        $path = database_path("tenants/{$db}.sqlite");
        if (!file_exists($path)) {
            throw new \Exception("SQLite não encontrado: $path");
        }
        config(['database.connections.tenant' => [
            'driver'                  => 'sqlite',
            'database'                => $path,
            'prefix'                  => '',
            'foreign_key_constraints' => true,
        ]]);
        DB::purge('tenant');
        DB::reconnect('tenant');
    }

    /**
     * Provide BI connection details.
     */
    public function connectBI(Request $request)
    {
        Log::info('ReceiveData /connectbi', ['payload' => $request->all(), 'headers' => $request->headers->all()]);
        $data = $request->validate([
            'user_identifier' => 'required|string',
            'database_name'   => 'nullable|string',
        ]);
        try {
            $client = Client::where('id', $data['user_identifier'])
                ->orWhere('database_name', $data['user_identifier'])
                ->firstOrFail();
        } catch (\Exception $e) {
            return response()->json(['error' => 'Cliente não encontrado'], 404);
        }
        $this->connectToTenant($client);
        $tables = Schema::connection('tenant')->getAllTables();
        $meta = array_map(function ($t) {
            $name = is_object($t) ? array_values((array)$t)[0] : $t;
            return ['table' => $name, 'columns' => Schema::connection('tenant')->getColumnListing($name)];
        }, $tables);
        return response()->json(['connection' => config('database.connections.tenant'), 'tables' => $meta]);
    }
}
