<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use App\Models\Client;

class DropProblematicTable extends Command
{
    protected $signature = 'table:drop {client_id} {table_name}';
    protected $description = 'Drop a problematic table from tenant database';

    public function handle()
    {
        $clientId = $this->argument('client_id');
        $tableName = $this->argument('table_name');

        try {
            // Find client
            $client = Client::where('id', $clientId)
                ->orWhere('database_name', $clientId)
                ->firstOrFail();

            // Connect to tenant database
            $this->connectToTenant($client);

            // Check if table exists
            if (Schema::connection('tenant')->hasTable($tableName)) {
                Schema::connection('tenant')->drop($tableName);
                $this->info("Tabela '{$tableName}' removida com sucesso do banco '{$client->database_name}'.");
            } else {
                $this->warn("Tabela '{$tableName}' não existe no banco '{$client->database_name}'.");
            }

        } catch (\Exception $e) {
            $this->error("Erro: " . $e->getMessage());
            return 1;
        }

        return 0;
    }

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
            // SQLite fallback
            $path = database_path("tenants/{$database}.sqlite");
            if (!file_exists($path)) {
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
    }
}