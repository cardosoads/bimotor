<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\Client;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Schema;
use Exception;
use Throwable;

class CloneController extends Controller
{
    /**
     * Clone database tables between clients
     */
    public function clone(Request $request)
    {
        $startTime = microtime(true);
        
        Log::info('Requisição /clone iniciada', [
            'timestamp' => now()->toISOString(),
            'user_agent' => $request->header('User-Agent'),
            'ip' => $request->ip()
        ]);

        try {
            $data = $request->validate([
                'source_client' => 'required|string|max:255',
                'target_client' => 'required|string|max:255',
                'tables' => 'required|array|min:1',
                'tables.*' => 'string|max:255',
                'include_structure' => 'boolean',
                'include_data' => 'boolean',
                'overwrite' => 'boolean',
            ]);
        } catch (Exception $e) {
            return $this->errorResponse('Dados de entrada inválidos: ' . $e->getMessage(), 400);
        }

        // Validar se source e target são diferentes
        if ($data['source_client'] === $data['target_client']) {
            return $this->errorResponse('Cliente de origem e destino devem ser diferentes', 400);
        }

        try {
            // Encontrar clientes
            $sourceClient = $this->findClient($data['source_client']);
            $targetClient = $this->findClient($data['target_client']);
            
            // Configurar conexões
            $this->connectToTenant($sourceClient, 'source');
            $this->connectToTenant($targetClient, 'target');
            
            // Testar conexões
            DB::connection('source')->getPdo();
            DB::connection('target')->getPdo();
            
        } catch (Exception $e) {
            Log::error('Erro na configuração inicial da clonagem', [
                'source' => $data['source_client'],
                'target' => $data['target_client'],
                'error' => $e->getMessage()
            ]);
            
            if (str_contains($e->getMessage(), 'não encontrado')) {
                return $this->errorResponse('Cliente não encontrado', 404);
            }
            
            return $this->errorResponse('Falha na conexão com os bancos de dados', 500);
        }

        $includeStructure = $data['include_structure'] ?? true;
        $includeData = $data['include_data'] ?? true;
        $overwrite = $data['overwrite'] ?? false;
        $tables = $data['tables'];

        $targetConn = DB::connection('target');
        $targetConn->beginTransaction();

        $clonedTables = [];
        $errors = [];
        $totalRows = 0;

        try {
            foreach ($tables as $tableName) {
                $tableStartTime = microtime(true);
                
                Log::info("Iniciando clonagem da tabela: {$tableName}");
                
                try {
                    // Verificar se a tabela existe na origem
                    if (!Schema::connection('source')->hasTable($tableName)) {
                        $errors[] = "Tabela '{$tableName}' não existe no banco de origem";
                        continue;
                    }
                    
                    // Verificar se a tabela já existe no destino
                    $tableExistsInTarget = Schema::connection('target')->hasTable($tableName);
                    
                    if ($tableExistsInTarget && !$overwrite) {
                        $errors[] = "Tabela '{$tableName}' já existe no destino (use a opção overwrite)";
                        continue;
                    }
                    
                    // Clonar estrutura se solicitado
                    if ($includeStructure) {
                        $this->cloneTableStructure($tableName, $overwrite);
                    }
                    
                    // Clonar dados se solicitado
                    $rowsCloned = 0;
                    if ($includeData) {
                        $rowsCloned = $this->cloneTableData($tableName, $overwrite);
                        $totalRows += $rowsCloned;
                    }
                    
                    $clonedTables[] = $tableName;
                    
                    $tableTime = microtime(true) - $tableStartTime;
                    Log::info("Tabela {$tableName} clonada com sucesso", [
                        'time' => round($tableTime, 2) . 's',
                        'rows' => $rowsCloned
                    ]);
                    
                } catch (Exception $e) {
                    $errors[] = "Erro ao clonar tabela '{$tableName}': " . $e->getMessage();
                    Log::error("Erro na clonagem da tabela {$tableName}", [
                        'error' => $e->getMessage(),
                        'trace' => $e->getTraceAsString()
                    ]);
                }
            }

            $targetConn->commit();

            $totalTime = microtime(true) - $startTime;
            
            Log::info('Clonagem concluída', [
                'source_client' => $sourceClient->database_name,
                'target_client' => $targetClient->database_name,
                'cloned_tables' => count($clonedTables),
                'total_rows' => $totalRows,
                'errors' => count($errors),
                'total_time' => round($totalTime, 2) . 's'
            ]);
            
            $summary = count($clonedTables) . " tabelas clonadas com sucesso";
            if (count($errors) > 0) {
                $summary .= ", " . count($errors) . " erros encontrados";
            }
            
            return $this->successResponse([
                'message' => count($clonedTables) > 0 ? 'Clonagem concluída' : 'Nenhuma tabela foi clonada',
                'summary' => $summary,
                'details' => array_merge(
                    array_map(function($table) { return "✅ Tabela '{$table}' clonada"; }, $clonedTables),
                    array_map(function($error) { return "❌ {$error}"; }, $errors)
                ),
                'statistics' => [
                    'cloned_tables' => count($clonedTables),
                    'total_rows' => $totalRows,
                    'errors' => count($errors),
                    'processing_time' => round($totalTime, 2),
                    'source_client' => $sourceClient->database_name,
                    'target_client' => $targetClient->database_name
                ]
            ]);

        } catch (Throwable $e) {
            $targetConn->rollBack();

            $errorTime = microtime(true) - $startTime;
            Log::error('Erro na transação de clonagem - rollback executado', [
                'source_client' => $sourceClient->database_name,
                'target_client' => $targetClient->database_name,
                'cloned_tables' => count($clonedTables),
                'error' => $e->getMessage(),
                'error_time' => round($errorTime, 2) . 's',
                'trace' => $e->getTraceAsString(),
            ]);
            
            return $this->errorResponse('Erro na clonagem: ' . $e->getMessage(), 500);
        }
    }

    /**
     * Clone table structure from source to target
     */
    private function cloneTableStructure(string $tableName, bool $overwrite = false): void
    {
        // Obter colunas da tabela de origem
        $sourceColumns = Schema::connection('source')->getColumnListing($tableName);
        
        if (empty($sourceColumns)) {
            throw new Exception("Não foi possível obter as colunas da tabela '{$tableName}'");
        }
        
        // Se overwrite estiver ativo e a tabela existir, removê-la
        if ($overwrite && Schema::connection('target')->hasTable($tableName)) {
            Schema::connection('target')->dropIfExists($tableName);
        }
        
        // Criar a tabela no destino
        Schema::connection('target')->create($tableName, function ($table) use ($sourceColumns, $tableName) {
            // Obter informações detalhadas das colunas
            $columnDetails = $this->getColumnDetails($tableName);
            
            foreach ($columnDetails as $column) {
                $this->addColumnToBlueprint($table, $column);
            }
            
            // Adicionar timestamps se não existirem
            if (!in_array('created_at', $sourceColumns) && !in_array('updated_at', $sourceColumns)) {
                $table->timestamps();
            }
        });
    }

    /**
     * Clone table data from source to target
     */
    private function cloneTableData(string $tableName, bool $overwrite = false): int
    {
        // Se overwrite estiver ativo, limpar dados da tabela de destino
        if ($overwrite && Schema::connection('target')->hasTable($tableName)) {
            // Para SQLite, usar DELETE ao invés de TRUNCATE para evitar problemas com sqlite_sequence
            $driver = config('database.connections.target.driver', config('database.default'));
            if ($driver === 'sqlite') {
                DB::connection('target')->table($tableName)->delete();
            } else {
                DB::connection('target')->table($tableName)->truncate();
            }
        }
        
        // Obter dados da tabela de origem
        $sourceData = DB::connection('source')->table($tableName)->get();
        
        if ($sourceData->isEmpty()) {
            return 0;
        }
        
        // Inserir dados na tabela de destino em lotes
        $batchSize = 500;
        $chunks = $sourceData->chunk($batchSize);
        $totalRows = 0;
        
        foreach ($chunks as $chunk) {
            $data = $chunk->map(function ($row) {
                return (array) $row;
            })->toArray();
            
            DB::connection('target')->table($tableName)->insert($data);
            $totalRows += count($data);
        }
        
        return $totalRows;
    }

    /**
     * Get detailed column information from source table
     */
    private function getColumnDetails(string $tableName): array
    {
        $columns = [];
        $columnListing = Schema::connection('source')->getColumnListing($tableName);
        
        foreach ($columnListing as $columnName) {
            $columnType = Schema::connection('source')->getColumnType($tableName, $columnName);
            
            $columns[] = [
                'name' => $columnName,
                'type' => $columnType,
                'nullable' => true, // Simplificado - pode ser melhorado
            ];
        }
        
        return $columns;
    }

    /**
     * Add column to blueprint based on column details
     */
    private function addColumnToBlueprint($table, array $column): void
    {
        $name = $column['name'];
        $type = $column['type'];
        
        // Mapear tipos de dados
        switch (strtolower($type)) {
            case 'integer':
            case 'int':
                $col = $table->integer($name);
                break;
            case 'bigint':
                $col = $table->bigInteger($name);
                break;
            case 'varchar':
            case 'string':
                $col = $table->string($name);
                break;
            case 'text':
                $col = $table->text($name);
                break;
            case 'longtext':
                $col = $table->longText($name);
                break;
            case 'datetime':
                $col = $table->dateTime($name);
                break;
            case 'timestamp':
                $col = $table->timestamp($name);
                break;
            case 'date':
                $col = $table->date($name);
                break;
            case 'decimal':
                $col = $table->decimal($name);
                break;
            case 'float':
                $col = $table->float($name);
                break;
            case 'double':
                $col = $table->double($name);
                break;
            case 'boolean':
                $col = $table->boolean($name);
                break;
            case 'json':
                $col = $table->json($name);
                break;
            default:
                $col = $table->string($name);
                break;
        }
        
        // Aplicar nullable se necessário
        if ($column['nullable'] ?? true) {
            $col->nullable();
        }
    }

    /**
     * Find client by identifier
     */
    private function findClient(string $identifier): Client
    {
        $client = Client::where('id', $identifier)
            ->orWhere('database_name', $identifier)
            ->orWhere('slug', $identifier)
            ->first();

        if (!$client) {
            throw new Exception("Cliente '{$identifier}' não encontrado");
        }

        return $client;
    }

    /**
     * Connect to tenant database
     */
    private function connectToTenant(Client $client, string $connectionName = 'tenant'): void
    {
        $client->connectToDatabase($connectionName);
    }

    /**
     * Return success response
     */
    private function successResponse(array $data, int $status = 200)
    {
        return response()->json(array_merge(['success' => true], $data), $status);
    }

    /**
     * Return error response
     */
    private function errorResponse(string $message, int $status = 400)
    {
        return response()->json([
            'success' => false,
            'message' => $message
        ], $status);
    }
}