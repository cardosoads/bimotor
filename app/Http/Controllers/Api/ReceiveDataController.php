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
    public function store(Request $request)
    {
        Log::info('Requisição /receive', ['payload' => $request->all()]);

        $data = $request->validate([
            'user_identifier' => 'required|string',
            'payload'         => 'required|array',
            'payload.*'       => 'array',
        ]);

        // Connect tenant
        $client = Client::where('id', $data['user_identifier'])
            ->orWhere('database_name', $data['user_identifier'])
            ->firstOrFail();
        $this->connectToTenant($client);

        $conn = DB::connection('tenant');
        $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, false);
        $conn->beginTransaction();

        try {
            foreach ($data['payload'] as $rawTable => $rows) {
                $table = $this->sanitizeTableName($rawTable);
                if (empty($rows)) continue;

                Log::info('Processando tabela', ['table' => $table, 'rows' => count($rows)]);

                $columnTypes = $this->inferColumnTypes($rows);

                if (Schema::connection('tenant')->hasTable($table)) {
                    $existingCols = Schema::connection('tenant')->getColumnListing($table);
                    $newCols = array_keys($columnTypes);
                    sort($existingCols);
                    sort($newCols);

                    if ($existingCols !== $newCols) {
                        Schema::connection('tenant')->dropIfExists($table);
                        $this->createTableWithTypes($table, $columnTypes);
                    }
                } else {
                    $this->createTableWithTypes($table, $columnTypes);
                }

                DB::connection('tenant')->statement("ALTER TABLE `$table` ROW_FORMAT=DYNAMIC");
                $conn->table($table)->truncate();

                foreach (array_chunk($rows, 500) as $batch) {
                    $conn->table($table)->insert($batch);
                }

                Log::info('Dados gravados', ['table' => $table, 'count' => count($rows)]);
            }

            $conn->commit();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            return response()->json(['message' => 'Sincronização completa']);
        } catch (\Throwable $e) {
            $conn->rollBack();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::error('Erro', ['error' => $e->getMessage()]);
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }

    protected function inferColumnTypes(array $rows): array
    {
        $columns = array_keys($rows[0]);
        $types = [];
        foreach ($columns as $col) {
            $maxInt = 0; $maxLen = 0;
            foreach ($rows as $row) {
                $v = $row[$col] ?? null;
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
                    default:
                        $t->string($col, 191)->nullable();
                        break;
                }
            }
            $t->timestamps();
            $t->timestamp('synced_at')->nullable();
        });
    }

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
                    default:
                        $t->string($col, 191)->nullable()->after('id');
                        break;
                }
            }
        });
    }

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

    protected function connectToTenant(Client $client)
    {
        config(['database.connections.tenant' => [
            'driver'    => 'mysql',
            'host'      => env('DB_HOST','127.0.0.1'),
            'port'      => env('DB_PORT','3306'),
            'database'  => $client->database_name,
            'username'  => env('DB_USERNAME'),
            'password'  => env('DB_PASSWORD'),
            'charset'   => 'utf8mb4',
            'collation' => 'utf8mb4_unicode_ci',
            'strict'    => true,
        ]]);
        DB::purge('tenant');
        DB::reconnect('tenant');
    }

    public function connectBI(Request $request)
    {
        Log::info('ReceiveData /connectbi', ['payload' => $request->all()]);

        $data = $request->validate([
            'user_identifier' => 'required|string',
            'database_name'   => 'nullable|string',
        ]);

        $client = Client::where('id', $data['user_identifier'])
            ->orWhere('database_name', $data['user_identifier'])
            ->firstOrFail();
        $this->connectToTenant($client);

        $tables = Schema::connection('tenant')->getAllTables();
        $meta = array_map(function ($t) {
            $name = is_object($t) ? array_values((array)$t)[0] : $t;
            return ['table' => $name, 'columns' => Schema::connection('tenant')->getColumnListing($name)];
        }, $tables);

        return response()->json([
            'connection' => config('database.connections.tenant'),
            'tables'     => $meta,
        ]);
    }
}
