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
use Exception;
use PDOException;
use Throwable;

class ReceiveDataController extends Controller
{
    // Configurações e constantes
    private const DEFAULT_BATCH_SIZE = 500;
    private const MAX_BATCH_SIZE = 1000;
    private const MIN_BATCH_SIZE = 100;
    private const MAX_ROW_SIZE_BYTES = 50000;
    private const MYSQL_MAX_ROW_SIZE = 65535;
    
    // Cache para esquemas de tabela
    private static array $schemaCache = [];
    private static array $connectionCache = [];
    
    /**
     * Store incoming data payload into the client-specific database.
     * Versão otimizada com melhor tratamento de erros e performance.
     */
    public function store(Request $request)
    {
        $startTime = microtime(true);
        $memoryStart = memory_get_usage(true);
        
        Log::info('Requisição /receive iniciada', [
            'timestamp' => now()->toISOString(),
            'memory_start' => $this->formatBytes($memoryStart),
            'user_agent' => $request->header('User-Agent'),
            'ip' => $request->ip()
        ]);

        try {
            $data = $request->validate([
                'user_identifier' => 'required|string|max:255',
                'payload'         => 'required|array|min:1',
                'payload.*'       => 'array',
            ]);
            
            // Validação adicional do payload
            $this->validatePayloadSize($data['payload']);
            
        } catch (Exception $e) {
            return $this->errorResponse('Dados de entrada inválidos: ' . $e->getMessage(), 400);
        }

        // Find client and connect tenant DB
        try {
            $client = $this->findClient($data['user_identifier']);
            $this->connectToTenant($client);
            
            // Teste de conexão
            DB::connection('tenant')->getPdo();
            
        } catch (Exception $e) {
            Log::error('Erro na configuração inicial', [
                'identifier' => $data['user_identifier'],
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            
            if (str_contains($e->getMessage(), 'não encontrado')) {
                return $this->errorResponse('Cliente não encontrado', 404);
            }
            
            return $this->errorResponse('Falha na conexão com o banco de dados', 500);
        }

        $conn = DB::connection('tenant');
        $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, false);
        $conn->beginTransaction();

        $processedTables = 0;
        $totalRows = 0;
        $skippedTables = [];

        try {
            foreach ($data['payload'] as $rawTable => $rows) {
                $table = $this->sanitizeTableName($rawTable);
                
                if (empty($rows)) {
                    $skippedTables[] = $table;
                    Log::info("Ignorando tabela vazia: $table");
                    continue;
                }
                
                $tableStartTime = microtime(true);
                Log::info('Processando tabela', [
                    'table' => $table, 
                    'rows' => count($rows),
                    'memory_usage' => $this->formatBytes(memory_get_usage(true))
                ]);

                // Remove 'id' before inferring types
                $cleanRows = array_map(function ($row) {
                    unset($row['id']);
                    return $row;
                }, $rows);

                $columnTypes = $this->getOrInferColumnTypes($table, $cleanRows);

                if (!Schema::connection('tenant')->hasTable($table)) {
                    $this->createTableWithTypes($table, $columnTypes);
                } else {
                    $this->updateTableSchemaWithTypes($table, $columnTypes);
                }

                // Upsert rows including 'id'
                $cols = $this->getTableColumns($table);
                $pk = $this->guessPrimaryKey($cols, $table);
                $filtered = $this->filterRows($rows, $cols, $pk);
                $processed = $this->processAndConvertData($filtered, $columnTypes, $table);
                $unique = $this->detectPrimaryKey($table, $cols);
                $updateCols = array_diff($cols, array_merge((array) $unique, ['created_at','updated_at','synced_at']));

                // Lotes dinâmicos baseados no tamanho dos dados
                $batchSize = $this->calculateOptimalBatchSize($processed);
                $batches = array_chunk($processed, $batchSize);
                
                foreach ($batches as $batchIndex => $batch) {
                    try {
                        DB::connection('tenant')->table($table)
                            ->upsert($batch, (array) $unique, $updateCols);
                    } catch (Exception $e) {
                        Log::error("Erro no lote {$batchIndex} da tabela {$table}", [
                            'error' => $e->getMessage(),
                            'batch_size' => count($batch)
                        ]);
                        throw $e;
                    }
                }
                
                $processedTables++;
                $totalRows += count($rows);
                
                $tableTime = microtime(true) - $tableStartTime;
                Log::info("Tabela {$table} processada", [
                    'time' => round($tableTime, 2) . 's',
                    'rows' => count($rows),
                    'batches' => count($batches)
                ]);
            }

            $conn->commit();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);

            $totalTime = microtime(true) - $startTime;
            $memoryPeak = memory_get_peak_usage(true);
            
            Log::info('Sincronização concluída com sucesso', [
                'client_id' => $client->id,
                'processed_tables' => $processedTables,
                'total_rows' => $totalRows,
                'skipped_tables' => count($skippedTables),
                'total_time' => round($totalTime, 2) . 's',
                'memory_peak' => $this->formatBytes($memoryPeak),
                'avg_rows_per_second' => $totalTime > 0 ? round($totalRows / $totalTime) : 0
            ]);
            
            return $this->successResponse([
                'message' => 'Dados sincronizados com sucesso',
                'summary' => [
                    'processed_tables' => $processedTables,
                    'total_rows' => $totalRows,
                    'processing_time' => round($totalTime, 2),
                    'skipped_tables' => $skippedTables
                ]
            ]);

        } catch (Throwable $e) {
            $conn->rollBack();
            $conn->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);

            $errorTime = microtime(true) - $startTime;
            Log::error('Erro na transação - rollback executado', [
                'client_id' => $client->id,
                'processed_tables' => $processedTables,
                'error' => $e->getMessage(),
                'error_time' => round($errorTime, 2) . 's',
                'trace' => $e->getTraceAsString(),
            ]);
            
            return $this->errorResponse('Erro no processamento: ' . $e->getMessage(), 500);
        }
    }

    /**
     * Métodos auxiliares adicionados para melhor organização
     */
    
    /** Validar tamanho do payload */
    protected function validatePayloadSize(array $payload): void
    {
        $totalRows = 0;
        $totalTables = count($payload);
        
        foreach ($payload as $table => $rows) {
            if (!is_array($rows)) {
                throw new Exception("Dados inválidos para a tabela: {$table}");
            }
            $totalRows += count($rows);
        }
        
        if ($totalTables > 100) {
            throw new Exception("Muitas tabelas no payload: {$totalTables} (máximo: 100)");
        }
        
        if ($totalRows > 50000) {
            throw new Exception("Muitas linhas no payload: {$totalRows} (máximo: 50.000)");
        }
        
        Log::info('Payload validado', [
            'total_tables' => $totalTables,
            'total_rows' => $totalRows
        ]);
    }
    
    /** Encontrar cliente com cache */
    protected function findClient(string $identifier): Client
    {
        $cacheKey = "client_{$identifier}";
        
        if (isset(self::$connectionCache[$cacheKey])) {
            return self::$connectionCache[$cacheKey];
        }
        
        $client = Client::where('id', $identifier)
            ->orWhere('database_name', $identifier)
            ->first();
            
        if (!$client) {
            throw new Exception("Cliente não encontrado: {$identifier}");
        }
        
        self::$connectionCache[$cacheKey] = $client;
        return $client;
    }
    
    /** Obter ou inferir tipos de coluna com cache */
    protected function getOrInferColumnTypes(string $table, array $rows): array
    {
        $cacheKey = "schema_{$table}";
        
        if (isset(self::$schemaCache[$cacheKey])) {
            Log::debug("Usando schema em cache para tabela: {$table}");
            return self::$schemaCache[$cacheKey];
        }
        
        $types = $this->inferColumnTypes($rows);
        self::$schemaCache[$cacheKey] = $types;
        
        return $types;
    }
    
    /** Obter colunas da tabela com cache */
    protected function getTableColumns(string $table): array
    {
        $cacheKey = "columns_{$table}";
        
        if (isset(self::$schemaCache[$cacheKey])) {
            return self::$schemaCache[$cacheKey];
        }
        
        $columns = Schema::connection('tenant')->getColumnListing($table);
        self::$schemaCache[$cacheKey] = $columns;
        
        return $columns;
    }
    
    /** Calcular tamanho ótimo do lote */
    protected function calculateOptimalBatchSize(array $data): int
    {
        if (empty($data)) {
            return self::DEFAULT_BATCH_SIZE;
        }
        
        // Estimar tamanho médio de linha
        $sampleRow = reset($data);
        $estimatedRowSize = 0;
        
        foreach ($sampleRow as $value) {
            if (is_string($value)) {
                $estimatedRowSize += mb_strlen($value);
            } elseif (is_numeric($value)) {
                $estimatedRowSize += 8; // Aproximação para números
            } else {
                $estimatedRowSize += 50; // Valor padrão
            }
        }
        
        // Calcular lote baseado no tamanho estimado
        if ($estimatedRowSize > 1000) {
            $batchSize = max(self::MIN_BATCH_SIZE, min(200, self::DEFAULT_BATCH_SIZE));
        } elseif ($estimatedRowSize > 500) {
            $batchSize = max(self::MIN_BATCH_SIZE, min(300, self::DEFAULT_BATCH_SIZE));
        } else {
            $batchSize = min(self::MAX_BATCH_SIZE, self::DEFAULT_BATCH_SIZE);
        }
        
        Log::debug("Tamanho de lote calculado", [
            'estimated_row_size' => $estimatedRowSize,
            'batch_size' => $batchSize,
            'total_rows' => count($data)
        ]);
        
        return $batchSize;
    }
    
    /** Formatar bytes para leitura humana */
    protected function formatBytes(int $bytes): string
    {
        $units = ['B', 'KB', 'MB', 'GB'];
        $bytes = max($bytes, 0);
        $pow = floor(($bytes ? log($bytes) : 0) / log(1024));
        $pow = min($pow, count($units) - 1);
        
        $bytes /= pow(1024, $pow);
        
        return round($bytes, 2) . ' ' . $units[$pow];
    }
    
    /** Resposta de sucesso padronizada */
    protected function successResponse(array $data, int $status = 200)
    {
        return response()->json(array_merge([
            'success' => true,
            'timestamp' => now()->toISOString()
        ], $data), $status);
    }
    
    /** Resposta de erro padronizada */
    protected function errorResponse(string $message, int $status = 500, array $details = [])
    {
        $response = [
            'success' => false,
            'error' => $message,
            'timestamp' => now()->toISOString()
        ];
        
        if (!empty($details)) {
            $response['details'] = $details;
        }
        
        return response()->json($response, $status);
    }

    /** Infer column types from rows */
    protected function inferColumnTypes(array $rows): array
    {
        $columns = array_keys($rows[0] ?? []);
        $types   = [];
        $totalColumns = count($columns);
        $samples = [];
        $nullCounts = [];
        
        // Se há muitas colunas, seja mais conservador com VARCHAR para evitar limite de linha
        $useTextForManyColumns = $totalColumns > 30;
        
        // Primeira passagem: coletar amostras e estatísticas
        foreach ($columns as $col) {
            $samples[$col] = [];
            $nullCounts[$col] = 0;
        }
        
        foreach ($rows as $row) {
            foreach ($columns as $col) {
                if (!isset($row[$col]) || $row[$col] === null || $row[$col] === '') {
                    $nullCounts[$col]++;
                    continue;
                }
                
                // Coletar amostras para análise mais precisa (máximo 20 amostras por coluna)
                if (count($samples[$col]) < 20) {
                    $samples[$col][] = $row[$col];
                }
            }
        }
        
        // Segunda passagem: inferir tipos baseado nas amostras
        foreach ($columns as $col) {
            if (empty($samples[$col])) {
                $types[$col] = 'longText'; // Usar longText para capacidade máxima
                continue;
            }
            
            $inferredType = $this->analyzeColumnSamples($samples[$col], $useTextForManyColumns);
            $types[$col] = $inferredType;
            
            Log::debug("Tipo inferido para coluna '{$col}'", [
                'type' => $inferredType,
                'samples_count' => count($samples[$col]),
                'null_count' => $nullCounts[$col]
            ]);
        }
        
        // Verificação adicional: log do tamanho estimado da linha
        $estimatedRowSize = $this->estimateRowSize($types);
        Log::info("Tamanho estimado da linha: $estimatedRowSize bytes (usando LONGTEXT para máxima capacidade)");
        
        return $types;
    }
    
    /**
     * Analisa amostras de uma coluna para inferir o tipo mais apropriado.
     */
    protected function analyzeColumnSamples(array $samples, bool $useTextForManyColumns): string
    {
        $typeScores = [
            'integer' => 0,
            'decimal' => 0,
            'timestamp' => 0,
            'boolean' => 0,
            'json' => 0,
            'email' => 0,
            'url' => 0,
            'string' => 0,
            'text' => 0
        ];
        
        $maxInt = 0;
        $maxLen = 0;
        $hasLongValues = false;
        
        foreach ($samples as $value) {
            $strValue = (string) $value;
            $len = mb_strlen($strValue);
            $maxLen = max($maxLen, $len);
            
            if ($len > 50) {
                $hasLongValues = true;
            }
            
            // Verificar tipos específicos
            if ($this->isStrictInteger($value)) {
                $typeScores['integer']++;
                $maxInt = max($maxInt, (int) $value);
            } elseif ($this->isDecimal($value)) {
                $typeScores['decimal']++;
            } elseif ($this->isTimestamp($value)) {
                $typeScores['timestamp']++;
            } elseif ($this->isBoolean($value)) {
                $typeScores['boolean']++;
            } elseif ($this->isJson($value)) {
                $typeScores['json']++;
            } elseif ($this->isEmail($value)) {
                $typeScores['email']++;
            } elseif ($this->isUrl($value)) {
                $typeScores['url']++;
            } elseif ($maxLen > 255 || $useTextForManyColumns) {
                $typeScores['text']++;
            } else {
                $typeScores['string']++;
            }
        }
        
        // Encontrar o tipo com maior pontuação
        $bestType = array_keys($typeScores, max($typeScores))[0];
        $confidence = max($typeScores) / count($samples);
        
        // Aplicar regras de decisão
        if ($confidence < 0.7 && $bestType !== 'string' && $bestType !== 'text') {
            // Se a confiança é baixa, usar longText como fallback para máxima capacidade
            $bestType = 'text';
        }
        
        // Retornar tipo específico baseado na análise
        switch ($bestType) {
            case 'integer':
                if ($maxInt > 2147483647) {
                    return 'bigInteger';
                } elseif ($maxInt > 32767) {
                    return 'integer';
                } elseif ($maxInt > 127) {
                    return 'smallInteger';
                } else {
                    return 'tinyInteger';
                }
            case 'decimal':
                return 'decimal';
            case 'timestamp':
                return 'timestamp';
            case 'boolean':
                return 'tinyInteger'; // MySQL não tem boolean nativo
            case 'json':
                return 'text'; // Armazenar JSON como TEXT
            case 'text':
            case 'string':
            default:
                // Para MySQL em produção, usar sempre LONGTEXT para máxima capacidade
                return 'longText';
        }
    }
    
    /** Verificar se é inteiro estrito */
    protected function isStrictInteger($value): bool
    {
        return is_numeric($value) && ctype_digit((string) $value) && strpos((string) $value, '.') === false;
    }
    
    /** Verificar se é decimal */
    protected function isDecimal($value): bool
    {
        return is_numeric($value) && strpos((string) $value, '.') !== false;
    }
    
    /** Verificar se é timestamp */
    protected function isTimestamp($value): bool
    {
        if (!is_string($value)) {
            return false;
        }
        
        // Verifica formatos de data comuns
        return preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/', $value) ||
               preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $value) ||
               (bool) strtotime($value);
    }
    
    /** Verificar se é boolean */
    protected function isBoolean($value): bool
    {
        $strValue = strtolower((string) $value);
        return in_array($strValue, ['true', 'false', '1', '0', 'yes', 'no', 'on', 'off']);
    }
    
    /** Verificar se é JSON */
    protected function isJson($value): bool
    {
        if (!is_string($value)) {
            return false;
        }
        
        json_decode($value);
        return json_last_error() === JSON_ERROR_NONE && (str_starts_with($value, '{') || str_starts_with($value, '['));
    }
    
    /** Verificar se é email */
    protected function isEmail($value): bool
    {
        return is_string($value) && filter_var($value, FILTER_VALIDATE_EMAIL) !== false;
    }
    
    /** Verificar se é URL */
    protected function isUrl($value): bool
    {
        return is_string($value) && filter_var($value, FILTER_VALIDATE_URL) !== false;
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
                // Tipos string limitados removidos
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

                    // Tipos string limitados removidos - usando longText para capacidade máxima
                    default:
                        $t->longText($column)->nullable(); // Usar LONGTEXT para capacidade máxima
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

                    // Tipos string limitados removidos - usando longText para capacidade máxima
                    default:
                        $t->longText($column)->nullable()->after('id'); // Usar LONGTEXT para capacidade máxima
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

    /** Process and convert data to match expected types with detailed logging */
    protected function processAndConvertData(array $rows, array $columnTypes, string $table): array
    {
        $conversionLog = [];
        
        return array_map(function ($row, $rowIndex) use ($columnTypes, $table, &$conversionLog) {
            $processed = [];
            
            foreach ($row as $key => $value) {
                $originalValue = $value;
                $convertedValue = $this->convertValueToExpectedType($value, $columnTypes[$key] ?? 'text', $key, $table, $rowIndex);
                
                // Log conversions
                if ($originalValue !== $convertedValue) {
                    $conversionLog[] = [
                        'table' => $table,
                        'row' => $rowIndex + 1,
                        'column' => $key,
                        'expected_type' => $columnTypes[$key] ?? 'text',
                        'original_value' => $originalValue,
                        'converted_value' => $convertedValue,
                        'conversion_reason' => $this->getConversionReason($originalValue, $convertedValue, $columnTypes[$key] ?? 'text')
                    ];
                }
                
                $processed[$key] = $convertedValue;
            }
            
            return $processed;
        }, $rows, array_keys($rows));
    }

    /** Convert value to expected type with automatic fallbacks */
    protected function convertValueToExpectedType($value, string $expectedType, string $column, string $table, int $rowIndex)
    {
        // Se valor é null ou vazio, retorna null
        if ($value === null || $value === '') {
            return null;
        }

        try {
            switch ($expectedType) {
                case 'timestamp':
                    return $this->convertToDateTime($value);
                    
                case 'bigInteger':
                case 'integer':
                case 'smallInteger':
                case 'tinyInteger':
                    return $this->convertToInteger($value);
                    
                case 'string20':
                    return $this->convertToString($value, 20);
                    
                case 'string30':
                    return $this->convertToString($value, 30);
                    
                case 'string50':
                    return $this->convertToString($value, 50);
                    
                case 'string100':
                    return $this->convertToString($value, 100);
                    
                case 'text':
                case 'mediumText':
                case 'longText':
                default:
                    return $this->convertToText($value);
            }
        } catch (\Exception $e) {
            Log::warning("Erro na conversão de dados", [
                'table' => $table,
                'row' => $rowIndex + 1,
                'column' => $column,
                'value' => $value,
                'expected_type' => $expectedType,
                'error' => $e->getMessage()
            ]);
            
            // Fallback: converter para string/text
            return $this->convertToText($value);
        }
    }

    /** Convert value to DateTime format */
    protected function convertToDateTime($value): ?string
    {
        if (is_string($value)) {
            try {
                // Tenta converter ISO 8601 para MySQL format
                if (preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/', $value)) {
                    $date = new \DateTime($value);
                    return $date->format('Y-m-d H:i:s');
                }
                
                // Se já está no formato MySQL, retorna como está
                if (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $value)) {
                    return $value;
                }
                
                // Tenta fazer parse genérico
                $date = new \DateTime($value);
                return $date->format('Y-m-d H:i:s');
                
            } catch (\Exception $e) {
                Log::warning("Falha ao converter data, usando NULL", [
                    'value' => $value,
                    'error' => $e->getMessage()
                ]);
                return null;
            }
        }
        
        return null;
    }

    /** Convert value to Integer */
    protected function convertToInteger($value): ?int
    {
        if (is_numeric($value)) {
            return (int) $value;
        }
        
        // Tenta extrair números da string
        if (is_string($value)) {
            $numbers = preg_replace('/[^0-9-]/', '', $value);
            if ($numbers !== '' && is_numeric($numbers)) {
                return (int) $numbers;
            }
        }
        
        Log::warning("Valor não numérico convertido para NULL", ['value' => $value]);
        return null;
    }

    /** Convert value to String with length limit */
    protected function convertToString($value, int $maxLength): ?string
    {
        if ($value === null) {
            return null;
        }
        
        $stringValue = (string) $value;
        
        // Se excede o limite, trunca e registra
        if (mb_strlen($stringValue) > $maxLength) {
            $truncated = mb_substr($stringValue, 0, $maxLength);
            Log::warning("String truncada para caber na coluna", [
                'original_length' => mb_strlen($stringValue),
                'max_length' => $maxLength,
                'original_value' => $stringValue,
                'truncated_value' => $truncated
            ]);
            return $truncated;
        }
        
        return $stringValue;
    }

    /** Convert value to Text */
    protected function convertToText($value): ?string
    {
        if ($value === null) {
            return null;
        }
        
        return (string) $value;
    }

    /** Get conversion reason for logging */
    protected function getConversionReason($original, $converted, string $expectedType): string
    {
        if ($original === null && $converted === null) {
            return 'Valor nulo mantido';
        }
        
        if ($converted === null) {
            return 'Valor inválido convertido para NULL';
        }
        
        switch ($expectedType) {
            case 'timestamp':
                return 'Data convertida do formato ISO 8601 para MySQL';
                
            case 'bigInteger':
            case 'integer':
            case 'smallInteger':
            case 'tinyInteger':
                return 'Valor convertido para inteiro';
                
            case 'string20':
            case 'string30':
            case 'string50':
            case 'string100':
                if (mb_strlen((string)$original) > mb_strlen((string)$converted)) {
                    return 'String truncada para caber no limite da coluna';
                }
                return 'Valor convertido para string';
                
            default:
                return 'Valor convertido para texto';
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
    protected function connectToTenant(Client $client): void
    {
        $database = $client->database_name;
        $connectionKey = "tenant_{$database}";
        
        // Verificar se já existe conexão em cache
        if (isset(self::$connectionCache[$connectionKey])) {
            Log::debug("Usando conexão em cache para: {$database}");
            return;
        }
        
        // Configuração MySQL com variáveis de ambiente
        $mysqlConfig = [
            'driver'    => 'mysql',
            'host'      => env('DB_HOST', env('MYSQL_HOST', 'localhost')),
            'port'      => env('DB_PORT', env('MYSQL_PORT', '3306')),
            'database'  => $database,
            'username'  => env('DB_USERNAME', env('MYSQL_USERNAME', 'root')),
            'password'  => env('DB_PASSWORD', env('MYSQL_PASSWORD', '')),
            'charset'   => env('DB_CHARSET', 'utf8mb4'),
            'collation' => env('DB_COLLATION', 'utf8mb4_unicode_ci'),
            'strict'    => env('DB_STRICT', true),
            'options'   => [
                \PDO::ATTR_TIMEOUT => 30,
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::MYSQL_ATTR_INIT_COMMAND => "SET sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'"
            ]
        ];
        
        config(['database.connections.tenant' => $mysqlConfig]);
        DB::purge('tenant');
        DB::reconnect('tenant');

        try {
            // Teste de conexão MySQL
            $pdo = DB::connection('tenant')->getPdo();
            $pdo->query('SELECT 1');
            
            Log::info("Conexão MySQL estabelecida", [
                'database' => $database,
                'host' => $mysqlConfig['host'],
                'port' => $mysqlConfig['port']
            ]);
            
            self::$connectionCache[$connectionKey] = true;
            return;
            
        } catch (Exception $e) {
            Log::warning('MySQL falhou, tentando SQLite fallback', [
                'database' => $database,
                'mysql_error' => $e->getMessage(),
                'mysql_host' => $mysqlConfig['host']
            ]);
        }

        // SQLite fallback
        $sqlitePath = $this->getSQLitePath($database);
        
        if (!file_exists($sqlitePath)) {
            // Tentar criar diretório se não existir
            $dir = dirname($sqlitePath);
            if (!is_dir($dir)) {
                if (!mkdir($dir, 0755, true)) {
                    throw new Exception("Não foi possível criar diretório SQLite: {$dir}");
                }
            }
            
            // Criar arquivo SQLite vazio
            touch($sqlitePath);
            Log::info("Arquivo SQLite criado: {$sqlitePath}");
        }
        
        $sqliteConfig = [
            'driver'                  => 'sqlite',
            'database'                => $sqlitePath,
            'prefix'                  => '',
            'foreign_key_constraints' => env('DB_FOREIGN_KEYS', true),
        ];
        
        config(['database.connections.tenant' => $sqliteConfig]);
        DB::purge('tenant');
        DB::reconnect('tenant');
        
        try {
            // Teste de conexão SQLite
            DB::connection('tenant')->getPdo()->query('SELECT 1');
            
            Log::info("Conexão SQLite estabelecida", [
                'database' => $database,
                'path' => $sqlitePath
            ]);
            
            self::$connectionCache[$connectionKey] = true;
            
        } catch (Exception $e) {
            throw new Exception("Falha em ambas as conexões (MySQL e SQLite): {$e->getMessage()}");
        }
    }
    
    /** Obter caminho do arquivo SQLite */
    protected function getSQLitePath(string $database): string
    {
        $baseDir = env('SQLITE_DATABASE_PATH', database_path('tenants'));
        return "{$baseDir}/{$database}.sqlite";
    }

    /**
     * Provide BI connection details.
     * Versão melhorada com tratamento de erros e cache.
     */
    public function connectBI(Request $request)
    {
        $startTime = microtime(true);
        
        Log::info('Requisição /connectbi iniciada', [
            'timestamp' => now()->toISOString(),
            'user_agent' => $request->header('User-Agent'),
            'ip' => $request->ip()
        ]);

        try {
            $data = $request->validate([
                'user_identifier' => 'required|string|max:255',
                'database_name'   => 'nullable|string|max:255',
            ]);

            $client = $this->findClient($data['user_identifier']);
            $this->connectToTenant($client);
            
            // Obter informações das tabelas
            $tables = Schema::connection('tenant')->getAllTables();
            $tableCount = count($tables);
            
            if ($tableCount === 0) {
                Log::warning('Nenhuma tabela encontrada no banco', [
                    'client_id' => $client->id,
                    'database' => $client->database_name
                ]);
            }

            $meta = array_map(function ($table) {
                $name = is_object($table) ? array_values((array) $table)[0] : $table;
                
                try {
                    $columns = $this->getTableColumns($name);
                    return [
                        'table'   => $name,
                        'columns' => $columns,
                        'column_count' => count($columns)
                    ];
                } catch (Exception $e) {
                    Log::warning("Erro ao obter colunas da tabela {$name}", [
                        'error' => $e->getMessage()
                    ]);
                    
                    return [
                        'table'   => $name,
                        'columns' => [],
                        'column_count' => 0,
                        'error' => 'Erro ao obter colunas'
                    ];
                }
            }, $tables);
            
            $processingTime = microtime(true) - $startTime;
            
            Log::info('Conexão BI estabelecida com sucesso', [
                'client_id' => $client->id,
                'database' => $client->database_name,
                'table_count' => $tableCount,
                'processing_time' => round($processingTime, 2) . 's'
            ]);

            // Remover informações sensíveis da configuração
            $connectionInfo = config('database.connections.tenant');
            unset($connectionInfo['password']);
            
            return $this->successResponse([
                'connection' => $connectionInfo,
                'tables' => $meta,
                'summary' => [
                    'database_name' => $client->database_name,
                    'table_count' => $tableCount,
                    'connection_type' => $connectionInfo['driver'],
                    'processing_time' => round($processingTime, 2)
                ]
            ]);
            
        } catch (Exception $e) {
            $errorTime = microtime(true) - $startTime;
            
            Log::error('Erro na conexão BI', [
                'error' => $e->getMessage(),
                'processing_time' => round($errorTime, 2) . 's',
                'trace' => $e->getTraceAsString()
            ]);
            
            if (str_contains($e->getMessage(), 'não encontrado')) {
                return $this->errorResponse('Cliente não encontrado', 404);
            }
            
            return $this->errorResponse('Erro na conexão BI: ' . $e->getMessage(), 500);
        }
    }
}
