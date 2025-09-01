<?php

namespace App\Console\Commands;

use App\Models\Client;
use Illuminate\Console\Command;

class CheckClientsCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'check:clients {--uuid= : Check specific UUID}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Check clients and diagnose UUID issues';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        $this->info('=== DIAGNÓSTICO DE CLIENTES ===');
        
        // Listar todos os clientes
        $clients = Client::all(['id', 'name', 'database_name']);
        
        $this->info("Total de clientes cadastrados: {$clients->count()}");
        $this->newLine();
        
        if ($clients->isEmpty()) {
            $this->warn('Nenhum cliente encontrado no banco de dados!');
            $this->info('Você precisa criar um cliente primeiro.');
            return;
        }
        
        // Mostrar todos os clientes
        $this->info('Clientes cadastrados:');
        foreach ($clients as $client) {
            $this->line("ID: {$client->id}");
            $this->line("Nome: {$client->name}");
            $this->line("Database: {$client->database_name}");
            $this->line('---');
        }
        
        // Verificar UUID específico se fornecido
        $uuid = $this->option('uuid');
        if ($uuid) {
            $this->newLine();
            $this->info("=== VERIFICANDO UUID: {$uuid} ===");
            
            // Buscar por ID
            $clientById = Client::find($uuid);
            if ($clientById) {
                $this->info('✅ Cliente encontrado por ID!');
                $this->line("Nome: {$clientById->name}");
                $this->line("Database: {$clientById->database_name}");
            } else {
                $this->error('❌ Cliente NÃO encontrado por ID');
            }
            
            // Buscar por database_name
            $clientByDb = Client::where('database_name', $uuid)->first();
            if ($clientByDb) {
                $this->info('✅ Cliente encontrado por database_name!');
                $this->line("ID: {$clientByDb->id}");
                $this->line("Nome: {$clientByDb->name}");
            } else {
                $this->error('❌ Cliente NÃO encontrado por database_name');
            }
            
            if (!$clientById && !$clientByDb) {
                $this->newLine();
                $this->error('PROBLEMA IDENTIFICADO: O UUID fornecido não corresponde a nenhum cliente!');
                $this->info('Soluções possíveis:');
                $this->line('1. Verificar se o UUID está correto no frontend');
                $this->line('2. Criar um cliente com este UUID');
                $this->line('3. Usar um dos UUIDs listados acima');
            }
        }
    }
}
