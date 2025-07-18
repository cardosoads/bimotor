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
        // Lê JSON via Laravel para garantir payload completo
        $raw = $request->json()->all();
        $payloadCount = is_array($raw['payload'] ?? null) ? count($raw['payload']) : 0;
        Log::info('Payload JSON recebido', ['tables' => $payloadCount]);

        // Se não vier JSON válido, loga o conteúdo bruto
        if (!$raw) {
            Log::error('Falha ao decodificar JSON', ['content' => $request->getContent()]);
            return response()->json(['error' => 'JSON inválido'], 400);
        }

        // Validação do conteúdo JSON
        $validator = Validator::make($raw, [
            'user_identifier' => 'required|string',
            'payload'         => 'required|array|min:1',
            'payload.*'       => 'array|min:1',
        ]);

        if ($validator->fails()) {
            Log::error('Validação inválida', ['errors' => $validator->errors()->all()]);
            return response()->json(['errors' => $validator->errors()], 422);
        }

        $data = $validator->validated();

        // Conecta no tenant apropriado
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
                $rowCount = is_array($rows) ? count($rows) : 0;
                if ($rowCount === 0) {
                    continue;
                }

                Log::info('Processando tabela', ['table' => $table, 'rows' => $rowCount]);

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

                $conn->statement("ALTER TABLE `{$table}` ROW_FORMAT=DYNAMIC");
                $conn->table($table)->truncate();

                // Inserções em batch de 1000 para performance
                foreach (array_chunk($rows, 1000) as $batch) {
                    $conn->table($table)->insert($batch);
                }

                Log::info('Dados gravados', ['table' => $table, 'count' => $rowCount]);
            }

            $conn->commit();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);

            return response()->json(['message' => 'Sincronização completa', 'tables' => $payloadCount]);
        } catch (\Throwable $e) {
            $conn->rollBack();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            Log::error('Erro na sincronização', ['error' => $e->getMessage()]);

            return response()->json(['error' => 'Falha na sincronização', 'details' => $e->getMessage()], 500);
        }
    }

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
            $name = is_object($t) ? array_values((array) $t)[0] : $t;
            return ['table' => $name, 'columns' => Schema::connection('tenant')->getColumnListing($name)];
        }, $tables);

        return response()->json([
            'connection' => config('database.connections.tenant'),
            'tables'     => $meta,
        ]);
    }

    public function verifyRecord(Request $request)
    {
        $data = $request->validate([
            'table' => 'required|string',
            'record_id' => 'required'
        ]);

        try {
            $this->connectToTenant(
                Client::where('id', $request->user_identifier)
                    ->orWhere('database_name', $request->user_identifier)
                    ->firstOrFail()
            );

            $table = $this->sanitizeTableName($data['table']);
            $exists = DB::connection('tenant')->table($table)->where('id', $data['record_id'])->exists();

            Log::info('Verificação de registro', ['table' => $table, 'record_id' => $data['record_id'], 'exists' => $exists]);
            return response()->json(['exists' => $exists]);
        } catch (\Throwable $e) {
            Log::error('Erro ao verificar registro', ['error' => $e->getMessage()]);
            return response()->json(['error' => 'Falha na verificação', 'details' => $e->getMessage()], 500);
        }
    }

    public function verifyTable($table, Request $request)
    {
        try {
            $this->connectToTenant(
                Client::where('id', $request->user_identifier)
                    ->orWhere('database_name', $request->user_identifier)
                    ->firstOrFail()
            );

            $table = $this->sanitizeTableName($table);
            $count = DB::connection('tenant')->table($table)->count();

            Log::info('Verificação de tabela', ['table' => $table, 'count' => $count]);
            return response()->json(['count' => $count]);
        } catch (\Throwable $e) {
            Log::error('Erro ao verificar tabela', ['error' => $e->getMessage()]);
            return response()->json(['error' => 'Falha na verificação', 'details' => $e->getMessage()], 500);
        }
    }
}
