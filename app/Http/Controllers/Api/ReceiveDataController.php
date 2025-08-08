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
        Log::info('Requisição /receive', [
            'payload' => $request->all(),
            'headers' => $request->headers->all(),
        ]);

        $data = $request->validate([
            'user_identifier' => 'required|string',
            'payload'         => 'required|array',
            'payload.*'       => 'array',
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

                // Remove 'id' before inferring types
                $cleanRows = array_map(function ($row) {
                    unset($row['id']);
                    return $row;
                }, $rows);

                $columnTypes = $this->inferColumnTypes($cleanRows);

                if (! Schema::connection('tenant')->hasTable($table)) {
                    $this->createTableWithTypes($table, $columnTypes);
                } else {
                    $this->updateTableSchemaWithTypes($table, $columnTypes);
                }

                // Upsert rows including 'id'
                $cols       = Schema::connection('tenant')->getColumnListing($table);
                $pk         = $this->guessPrimaryKey($cols, $table);
                $filtered   = $this->filterRows($rows, $cols, $pk);
                $processed  = $this->processDateTimeValues($filtered, $columnTypes);
                $unique     = $this->detectPrimaryKey($table, $cols);
                $updateCols = array_diff($cols, array_merge((array) $unique, ['created_at','updated_at','synced_at']));

                foreach (array_chunk($processed, 500) as $batch) {
                    DB::connection('tenant')->table($table)
                        ->upsert($batch, (array) $unique, $updateCols);
                }
            }

            $conn->commit();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);

            Log::info('Sincronização concluída', ['client_id' => $client->id]);
            return response()->json(['message' => 'Dados sincronizados com sucesso']);

        } catch (\Throwable $e) {
            $conn->rollBack();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);

            Log::error('Erro na transação', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }

    /** Infer column types from rows */
    protected function inferColumnTypes(array $rows): array
    {
        $columns = array_keys($rows[0] ?? []);
        $types   = [];
        $totalColumns = count($columns);
        
        // Se há muitas colunas, seja mais conservador com VARCHAR para evitar limite de linha
        $useTextForManyColumns = $totalColumns > 50;
        
        foreach ($columns as $col) {
            $maxInt = 0;
            $maxLen = 0;
            $isNumeric = true;
            $isDate = true;
            $hasLongValues = false;
            
            foreach ($rows as $row) {
                if (! isset($row[$col]) || $row[$col] === null || $row[$col] === '') {
                    continue;
                }
                $value = $row[$col];
                
                // Check if numeric
                if ($isNumeric && is_numeric($value)) {
                    if (ctype_digit((string) $value)) {
                        $maxInt = max($maxInt, (int) $value);
                    } else {
                        $isNumeric = false;
                    }
                } else {
                    $isNumeric = false;
                }
                
                // Check if date
                if ($isDate && !strtotime($value)) {
                    $isDate = false;
                }
                
                if (is_string($value)) {
                    $len = mb_strlen($value);
                    $maxLen = max($maxLen, $len);
                    if ($len > 100) {
                        $hasLongValues = true;
                    }
                }
            }
            
            // Determine optimal type - be more conservative to avoid truncation
            if ($isNumeric && $maxInt > 0) {
                if ($maxInt > 2147483647) {
                    $types[$col] = 'bigInteger';
                } elseif ($maxInt > 32767) {
                    $types[$col] = 'integer';
                } elseif ($maxInt > 127) {
                    $types[$col] = 'smallInteger';
                } else {
                    $types[$col] = 'tinyInteger';
                }
            } elseif ($isDate && $maxLen > 0) {
                $types[$col] = 'timestamp';
            } elseif ($maxLen > 65535) {
                $types[$col] = 'longText';
            } elseif ($maxLen > 16777215) {
                $types[$col] = 'mediumText';
            } elseif ($maxLen > 50 || $hasLongValues || $useTextForManyColumns) {
                // Be very conservative: use TEXT for values > 50 chars or when many columns
                $types[$col] = 'text';
            } elseif ($maxLen > 30) {
                $types[$col] = 'string100';
            } elseif ($maxLen > 20) {
                 $types[$col] = 'string50';
             } elseif ($maxLen > 15) {
                 $types[$col] = 'string30';
             } elseif ($maxLen > 10) {
                 $types[$col] = 'string20';
             } else {
                 $types[$col] = 'string20';
            }
        }
        
        // Verificação adicional: se estimativa de tamanho de linha for muito grande, converta strings para text
        $estimatedRowSize = $this->estimateRowSize($types);
        if ($estimatedRowSize > 50000) { // Margem de segurança antes do limite de 65535
            Log::warning("Tamanho estimado da linha muito grande ($estimatedRowSize bytes), convertendo strings para TEXT");
            foreach ($types as $col => $type) {
                if (in_array($type, ['string191', 'string100', 'string50', 'string30', 'string20'])) {
                    $types[$col] = 'text';
                }
            }
        }
        
        return $types;
    }
    
    /** Estimate row size in bytes */
    protected function estimateRowSize(array $types): int
    {
        $size = 0;
        foreach ($types as $type) {
            switch ($type) {
                case 'tinyInteger':
                    $size += 1;
                    break;
                case 'smallInteger':
                    $size += 2;
                    break;
                case 'integer':
                    $size += 4;
                    break;
                case 'bigInteger':
                    $size += 8;
                    break;
                case 'timestamp':
                    $size += 4;
                    break;
                case 'string20':
                    $size += 20;
                    break;
                case 'string30':
                    $size += 30;
                    break;
                case 'string50':
                    $size += 50;
                    break;
                case 'string100':
                    $size += 100;
                    break;
                case 'text':
                case 'mediumText':
                case 'longText':
                    $size += 10; // TEXT uses pointer
                    break;
                default:
                    $size += 50; // default
            }
        }
        return $size;
    }

    /** Create table with inferred column types */
    protected function createTableWithTypes(string $table, array $types)
    {
        Log::info("Criando tabela $table com " . count($types) . " colunas", ['types' => $types]);
        
        Schema::connection('tenant')->create($table, function (Blueprint $t) use ($types) {
            $t->id();
            foreach ($types as $column => $type) {
                switch ($type) {
                    case 'bigInteger':
                        $t->bigInteger($column)->nullable();
                        break;
                    case 'integer':
                        $t->integer($column)->nullable();
                        break;
                    case 'smallInteger':
                        $t->smallInteger($column)->nullable();
                        break;
                    case 'tinyInteger':
                        $t->tinyInteger($column)->nullable();
                        break;
                    case 'timestamp':
                        $t->timestamp($column)->nullable();
                        break;
                    case 'longText':
                        $t->longText($column)->nullable();
                        break;
                    case 'mediumText':
                        $t->mediumText($column)->nullable();
                        break;
                    case 'text':
                        $t->text($column)->nullable();
                        break;

                    case 'string100':
                        $t->string($column, 100)->nullable();
                        break;
                    case 'string50':
                        $t->string($column, 50)->nullable();
                        break;
                    case 'string30':
                        $t->string($column, 30)->nullable();
                        break;
                    case 'string20':
                        $t->string($column, 20)->nullable();
                        break;
                    default:
                        $t->text($column)->nullable(); // Mudança: usar TEXT como padrão ao invés de string(50)
                }
            }
            $t->timestamps();
            $t->timestamp('synced_at')->nullable();
            
            // Configurar engine e row format para suportar linhas grandes
            $t->engine = 'InnoDB';
        });
        
        // Configurar ROW_FORMAT=DYNAMIC após criação da tabela
        try {
            DB::connection('tenant')->statement("ALTER TABLE `$table` ROW_FORMAT=DYNAMIC");
            Log::info("ROW_FORMAT=DYNAMIC configurado para tabela $table");
        } catch (\Exception $e) {
            Log::warning("Falha ao configurar ROW_FORMAT=DYNAMIC para $table: " . $e->getMessage());
        }
    }

    /** Update existing table schema by adding new columns */
    protected function updateTableSchemaWithTypes(string $table, array $types)
    {
        $existing = Schema::connection('tenant')->getColumnListing($table);
        $newCols  = array_diff(array_keys($types), $existing);
        if (empty($newCols)) {
            return;
        }
        
        Log::info("Adicionando " . count($newCols) . " novas colunas à tabela $table", ['columns' => $newCols]);
        
        Schema::connection('tenant')->table($table, function (Blueprint $t) use ($types, $newCols) {
            foreach ($newCols as $column) {
                switch ($types[$column]) {
                    case 'bigInteger':
                        $t->bigInteger($column)->nullable()->after('id');
                        break;
                    case 'integer':
                        $t->integer($column)->nullable()->after('id');
                        break;
                    case 'smallInteger':
                        $t->smallInteger($column)->nullable()->after('id');
                        break;
                    case 'tinyInteger':
                        $t->tinyInteger($column)->nullable()->after('id');
                        break;
                    case 'timestamp':
                        $t->timestamp($column)->nullable()->after('id');
                        break;
                    case 'longText':
                        $t->longText($column)->nullable()->after('id');
                        break;
                    case 'mediumText':
                        $t->mediumText($column)->nullable()->after('id');
                        break;
                    case 'text':
                        $t->text($column)->nullable()->after('id');
                        break;

                    case 'string100':
                        $t->string($column, 100)->nullable()->after('id');
                        break;
                    case 'string50':
                        $t->string($column, 50)->nullable()->after('id');
                        break;
                    case 'string30':
                        $t->string($column, 30)->nullable()->after('id');
                        break;
                    case 'string20':
                        $t->string($column, 20)->nullable()->after('id');
                        break;
                    default:
                        $t->text($column)->nullable()->after('id'); // Mudança: usar TEXT como padrão
                }
            }
        });
    }

    /** Filter rows keeping only existing columns and primary key */
    protected function filterRows(array $rows, array $columns, ?string $pk): array
    {
        return array_map(function ($row) use ($columns, $pk) {
            $filtered = [];
            foreach ($row as $key => $value) {
                $colKey = ($pk && $key === $pk && in_array('id', $columns)) ? 'id' : $key;
                if (in_array($colKey, $columns)) {
                    $filtered[$colKey] = $value;
                }
            }
            return $filtered;
        }, $rows);
    }

    /** Process datetime values to convert ISO 8601 to MySQL format */
    protected function processDateTimeValues(array $rows, array $columnTypes): array
    {
        return array_map(function ($row) use ($columnTypes) {
            $processed = [];
            foreach ($row as $key => $value) {
                // Se a coluna é do tipo timestamp e o valor parece ser ISO 8601
                if (isset($columnTypes[$key]) && $columnTypes[$key] === 'timestamp' && is_string($value)) {
                    $processed[$key] = $this->convertIsoToMysqlDateTime($value);
                } else {
                    $processed[$key] = $value;
                }
            }
            return $processed;
        }, $rows);
    }

    /** Convert ISO 8601 datetime to MySQL format */
    protected function convertIsoToMysqlDateTime(string $isoDateTime): ?string
    {
        try {
            // Tenta converter ISO 8601 para MySQL format
            if (preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/', $isoDateTime)) {
                $date = new \DateTime($isoDateTime);
                return $date->format('Y-m-d H:i:s');
            }
            
            // Se já está no formato MySQL, retorna como está
            if (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $isoDateTime)) {
                return $isoDateTime;
            }
            
            // Tenta fazer parse genérico
            $date = new \DateTime($isoDateTime);
            return $date->format('Y-m-d H:i:s');
            
        } catch (\Exception $e) {
            Log::warning("Falha ao converter data: $isoDateTime", ['error' => $e->getMessage()]);
            return null;
        }
    }

    /** Detect primary key columns */
    protected function detectPrimaryKey(string $table, array $columns): array
    {
        if (in_array('id', $columns)) {
            return ['id'];
        }
        try {
            $schemaManager = Schema::connection('tenant')
                ->getConnection()->getDoctrineConnection()->getSchemaManager();
            $pk = $schemaManager->listTableDetails($table)->getPrimaryKey();
            return $pk ? $pk->getColumns() : [array_key_first($columns)];
        } catch (\Throwable $e) {
            return [array_key_first($columns)];
        }
    }

    /** Guess primary key name by convention */
    protected function guessPrimaryKey(array $columns, string $table): ?string
    {
        foreach ($columns as $col) {
            $lower = Str::lower($col);
            if ($lower === 'id' || $lower === $table.'_id') {
                return $col;
            }
        }
        return null;
    }

    /** Sanitize table name to safe characters */
    protected function sanitizeTableName(string $name): string
    {
        return preg_replace('/[^a-zA-Z0-9_]/', '', $name);
    }

    /** Configure tenant DB connection with MySQL fallback to SQLite */
    protected function connectToTenant(Client $client)
    {
        $database = $client->database_name;
        // MySQL configuration
        config(['database.connections.tenant' => [
            'driver'    => 'mysql',
            'host'      => env('DB_HOST', '127.0.0.1'),
            'port'      => env('DB_PORT', '3306'),
            'database'  => $database,
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
            Log::warning('MySQL falhou, tentando SQLite', ['db' => $database, 'error' => $e->getMessage()]);
        }

        // SQLite fallback
        $path = database_path("tenants/{$database}.sqlite");
        if (! file_exists($path)) {
            throw new \Exception("Arquivo SQLite não encontrado: {$path}");
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

        $client = Client::where('id', $data['user_identifier'])
            ->orWhere('database_name', $data['user_identifier'])
            ->firstOrFail();

        $this->connectToTenant($client);
        $tables = Schema::connection('tenant')->getAllTables();

        $meta = array_map(function ($table) {
            $name = is_object($table) ? array_values((array) $table)[0] : $table;
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
