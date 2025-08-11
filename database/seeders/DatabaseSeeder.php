<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\Client;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Schema;

class ExportDatabaseController extends Controller
{
    /**
     * Export complete database structure and data
     */
    public function exportComplete(Request $request)
    {
        $data = $request->validate([
            'user_identifier' => 'required|string',
            'tables' => 'array', // Se vazio, exporta todas as tabelas
            'include_structure' => 'boolean|nullable',
            'include_data' => 'boolean|nullable',
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
            
            $includeStructure = $data['include_structure'] ?? true;
            $includeData = $data['include_data'] ?? true;
            $selectedTables = $data['tables'] ?? [];
            
            // Obter todas as tabelas se nenhuma foi especificada
            if (empty($selectedTables)) {
                $allTables = Schema::connection('tenant')->getAllTables();
                $selectedTables = array_map(function($table) {
                    return is_object($table) ? array_values((array) $table)[0] : $table;
                }, $allTables);
            }
            
            $exportData = [];
            
            foreach ($selectedTables as $tableName) {
                Log::info("Exportando tabela: $tableName");
                
                $tableData = [
                    'name' => $tableName,
                    'structure' => null,
                    'data' => null,
                    'row_count' => 0
                ];
                
                // Exportar estrutura da tabela
                if ($includeStructure) {
                    $tableData['structure'] = $this->getTableStructure($tableName);
                }
                
                // Exportar dados da tabela
                if ($includeData) {
                    $tableData['data'] = $this->getTableData($tableName);
                    $tableData['row_count'] = count($tableData['data']);
                }
                
                $exportData[$tableName] = $tableData;
            }
            
            Log::info('Exportação completa concluída', [
                'client_id' => $client->id,
                'tables_exported' => count($exportData),
                'total_rows' => array_sum(array_column($exportData, 'row_count'))
            ]);
            
            return response()->json([
                'message' => 'Exportação concluída com sucesso',
                'client' => $client->database_name,
                'export_data' => $exportData,
                'summary' => [
                    'tables_count' => count($exportData),
                    'total_rows' => array_sum(array_column($exportData, 'row_count')),
                    'exported_at' => now()->toISOString()
                ]
            ]);
            
        } catch (\Throwable $e) {
            Log::error('Erro na exportação completa', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }
    
    /**
     * Get table structure (columns, types, indexes)
     */
    protected function getTableStructure(string $tableName): array
    {
        $columns = Schema::connection('tenant')->getColumnListing($tableName);
        $columnDetails = [];
        
        foreach ($columns as $column) {
            $columnType = Schema::connection('tenant')->getColumnType($tableName, $column);
            $columnDetails[$column] = [
                'type' => $columnType,
                'nullable' => true, // Simplificado - você pode melhorar isso
            ];
        }
        
        return [
            'columns' => $columnDetails,
            'primary_key' => $this->detectPrimaryKey($tableName, $columns),
        ];
    }
    
    /**
     * Get all table data
     */
    protected function getTableData(string $tableName): array
    {
        return DB::connection('tenant')->table($tableName)->get()->toArray();
    }
    
    /**
     * Import complete database from export data
     */
    public function importComplete(Request $request)
    {
        $data = $request->validate([
            'user_identifier' => 'required|string',
            'export_data' => 'required|array',
            'overwrite_existing' => 'boolean|nullable',
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
            $overwrite = $data['overwrite_existing'] ?? false;
            
            $conn = DB::connection('tenant');
            $conn->beginTransaction();
            
            $importedTables = 0;
            $importedRows = 0;
            
            foreach ($data['export_data'] as $tableName => $tableData) {
                Log::info("Importando tabela: $tableName");
                
                // Verificar se tabela já existe
                if (Schema::connection('tenant')->hasTable($tableName)) {
                    if ($overwrite) {
                        Schema::connection('tenant')->dropIfExists($tableName);
                        Log::info("Tabela $tableName removida para recriar");
                    } else {
                        Log::info("Tabela $tableName já existe, pulando...");
                        continue;
                    }
                }
                
                // Criar estrutura da tabela se fornecida
                if (isset($tableData['structure'])) {
                    $this->createTableFromStructure($tableName, $tableData['structure']);
                }
                
                // Importar dados se fornecidos
                if (isset($tableData['data']) && !empty($tableData['data'])) {
                    $this->importTableData($tableName, $tableData['data']);
                    $importedRows += count($tableData['data']);
                }
                
                $importedTables++;
            }
            
            $conn->commit();
            
            Log::info('Importação completa concluída', [
                'client_id' => $client->id,
                'tables_imported' => $importedTables,
                'rows_imported' => $importedRows
            ]);
            
            return response()->json([
                'message' => 'Importação concluída com sucesso',
                'tables_imported' => $importedTables,
                'rows_imported' => $importedRows
            ]);
            
        } catch (\Throwable $e) {
            $conn->rollBack();
            Log::error('Erro na importação completa', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }
    
    // ... métodos auxiliares (connectToTenant, detectPrimaryKey, etc.)
    // Copie os métodos necessários do controller.php original
}
