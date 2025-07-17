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
        // Decodifica JSON cru para evitar limites de max_input_vars
        $raw = json_decode($request->getContent(), true);
        Log::info('Payload bruto recebido', ['tables' => count($raw['payload'] ?? [])]);

        // Log detalhado por tabela
        foreach ($raw['payload'] ?? [] as $tableName => $rows) {
            Log::info('Tabela no payload bruto', ['table' => $tableName, 'rows' => count($rows)]);
        }

        // Validação após decodificar
        $validator = Validator::make($raw, [
            'user_identifier' => 'required|string',
            'payload'         => 'required|array',
            'payload.*'       => 'array',
        ]);

        $data = $validator->validate();

        // Conecta no tenant
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
                if (empty($rows)) {
                    continue;
                }

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

                DB::connection('tenant')->statement("ALTER TABLE `{$table}` ROW_FORMAT=DYNAMIC");
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
            Log::error('Erro na sincronização', ['error' => $e->getMessage()]);

            return response()->json(['error' => $e->getMessage()], 500);
        }
    }

    // ... demais métodos inalterados (inferColumnTypes, createTableWithTypes, etc.)

    protected function inferColumnTypes(array $rows): array
    {
        $columns = array_keys($rows[0]);
        $types = [];
        foreach ($columns as $col) {
            $maxInt = 0;
            $maxLen = 0;
            foreach ($rows as $row) {
                $v = $row[$col] ?? null;
                if (is_numeric($v) && ctype_digit((string) $v)) {
                    $maxInt = max($maxInt, (int) $v);
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

    protected function sanitizeTableName(string $name): string
    {
        return preg_replace('/[^a-zA-Z0-9_]/', '', $name);
    }

    protected function connectToTenant(Client $client)
    {
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
    }

    // public function connectBI() permanece inalterado...
}
