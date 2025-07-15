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
            'headers' => $request->headers->all(),
        ]);

        $data = $request->validate([
            'user_identifier' => 'required|string',
            'payload' => 'required|array',
            'payload.*' => 'array',
        ]);

        Log::info('ReceiveData payload', [
            'user_identifier' => $data['user_identifier'],
            'tables' => array_keys($data['payload']),
            'total_tables' => count($data['payload']),
            'total_rows' => array_sum(array_map('count', $data['payload'])),
        ]);

        // Find client
        try {
            $client = Client::where('id', $data['user_identifier'])
                ->orWhere('database_name', $data['user_identifier'])
                ->firstOrFail();
            Log::info('Cliente encontrado', ['id' => $client->id, 'db' => $client->database_name]);
        } catch (\Exception $e) {
            Log::error('Cliente não encontrado', ['identifier' => $data['user_identifier']]);
            return response()->json(['error' => 'Cliente não encontrado'], 404);
        }

        // Connect tenant
        try {
            $this->connectToTenant($client);
            DB::connection('tenant')->getPdo();
            Log::info('Conectado ao tenant', ['database' => $client->database_name]);
        } catch (\Exception $e) {
            Log::error('Erro conexão tenant', ['error' => $e->getMessage()]);
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

                if (!Schema::connection('tenant')->hasTable($table)) {
                    $this->createTable($table, $rows[0]);
                } else {
                    $this->updateTableSchema($table, $rows[0]);
                }

                // Prevent row-size errors
                DB::connection('tenant')->statement("ALTER TABLE `$table` ROW_FORMAT=DYNAMIC");

                $cols = Schema::connection('tenant')->getColumnListing($table);
                $pk = $this->guessPrimaryKey($cols, $table);
                $filtered = $this->filterRows($rows, $cols, $table, $pk);
                $unique = $this->detectPrimaryKey($table, $cols, $rows[0]);

                DB::connection('tenant')->table($table)->upsert(
                    $filtered,
                    (array) $unique,
                    array_diff($cols, array_merge((array) $unique, ['created_at','updated_at','synced_at']))
                );
                Log::info('Upsert concluído', ['table' => $table, 'count' => count($filtered)]);
            }

            $conn->commit();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::info('Sincronização finalizada', ['client_id' => $client->id]);

            return response()->json(['message' => 'Dados sincronizados com sucesso']);
        } catch (\Throwable $e) {
            $conn->rollBack();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::error('Erro na transação', ['error' => $e->getMessage(), 'trace' => $e->getTraceAsString()]);
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }

    /**
     * Create table dynamically.
     */
    protected function createTable(string $table, array $firstRow)
    {
        $cols = array_keys($firstRow);
        Schema::connection('tenant')->create($table, function (Blueprint $t) use ($firstRow, $cols) {
            $t->id();
            foreach ($cols as $col) {
                if (Str::lower($col) === 'id') continue;
                $val = $firstRow[$col];

                // Specific fix for numero_fechamento overflow
                if (Str::lower($col) === 'numero_fechamento') {
                    $t->bigInteger($col)->nullable();
                } elseif (count($cols) > 20 || in_array(Str::lower($col), ['cfop','cst','ncm','cnpj','cpf'])) {
                    // Use TEXT for most cols to avoid row size issues
                    $t->text($col)->nullable();
                } elseif ($this->isBoolean($val)) {
                    $t->boolean($col)->nullable();
                } elseif ($this->isDateTime($val)) {
                    $t->dateTime($col)->nullable();
                } elseif ($this->isDate($val)) {
                    $t->date($col)->nullable();
                } elseif (is_numeric($val) && (int)$val > 2147483647) {
                    $t->bigInteger($col)->nullable();
                } elseif (is_int($val)) {
                    $t->integer($col)->nullable();
                } elseif (is_float($val)) {
                    $t->float($col)->nullable();
                } else {
                    // default to TEXT
                    $t->text($col)->nullable();
                }
            }
            $t->timestamps();
            $t->timestamp('synced_at')->nullable();
        });
    }

    /**
     * Update table schema if new columns appear.
     */
    protected function updateTableSchema(string $table, array $firstRow)
    {
        $existing = Schema::connection('tenant')->getColumnListing($table);
        $new = array_diff(array_keys($firstRow), $existing);
        if (empty($new)) return;

        Schema::connection('tenant')->table($table, function (Blueprint $t) use ($new, $firstRow) {
            foreach ($new as $col) {
                if (Str::lower($col) === 'id') continue;
                $val = $firstRow[$col];

                if (Str::lower($col) === 'numero_fechamento') {
                    $t->bigInteger($col)->nullable()->after('id');
                } elseif (is_numeric($val) && (int)$val > 2147483647) {
                    $t->bigInteger($col)->nullable()->after('id');
                } elseif (in_array(Str::lower($col), ['cfop','cst','ncm','cnpj','cpf'])) {
                    $t->text($col)->nullable()->after('id');
                } elseif ($this->isBoolean($val)) {
                    $t->boolean($col)->nullable()->after('id');
                } elseif ($this->isDateTime($val)) {
                    $t->dateTime($col)->nullable()->after('id');
                } elseif ($this->isDate($val)) {
                    $t->date($col)->nullable()->after('id');
                } elseif (count($new) > 0) {
                    $t->text($col)->nullable()->after('id');
                }
            }
        });
    }

    /**
     * Filter rows to existing columns.
     */
    protected function filterRows(array $rows, array $cols, string $table, ?string $pk): array
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
     * Detect primary key via schema or heuristics.
     */
    protected function detectPrimaryKey(string $table, array $cols, array $row): array
    {
        if (in_array('id', $cols)) return ['id'];
        try {
            $sm = Schema::connection('tenant')->getConnection()->getDoctrineConnection()->getSchemaManager();
            $pk = $sm->listTableDetails($table)->getPrimaryKey();
            if ($pk) return $pk->getColumns();
        } catch (\Throwable $e) {
        }
        return [array_key_first($row)];
    }

    /**
     * Guess primary key by name.
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

    protected function isDateTime($v): bool
    {
        return is_string($v) && preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $v);
    }

    protected function isDate($v): bool
    {
        return is_string($v) && preg_match('/^\d{4}-\d{2}-\d{2}$/', $v);
    }

    protected function isBoolean($v): bool
    {
        return in_array($v, [0,1,'0','1',true,false], true);
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

        // Tenta MySQL
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
            Log::warning('MySQL falhou, tentando SQLite', ['db' => $db, 'error' => $e->getMessage()]);
        }

        // Fallback SQLite
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
        Log::info('ReceiveData /connectbi', [
            'payload' => $request->all(),
            'headers' => $request->headers->all(),
        ]);

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

        try {
            $this->connectToTenant($client);
        } catch (\Exception $e) {
            return response()->json(['error' => 'Falha na conexão com o tenant'], 500);
        }

        $tables = Schema::connection('tenant')->getAllTables();
        $meta = array_map(function ($t) {
            $name = is_object($t) ? array_values((array)$t)[0] : $t;
            return [
                'table'   => $name,
                'columns' => Schema::connection('tenant')->getColumnListing($name),
            ];
        }, $tables);

        return response()->json([
            'connection' => config('database.connections.tenant'),
            'tables'     => $meta,
        ]);
    }
}
