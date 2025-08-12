<?php

/**
 * Script de teste para a funcionalidade de clonagem
 * Este script demonstra como usar a API de clonagem entre clientes
 */

require_once 'vendor/autoload.php';

use App\Models\Client;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

// Configurar o ambiente Laravel
$app = require_once 'bootstrap/app.php';
$app->make('Illuminate\Contracts\Console\Kernel')->bootstrap();

echo "=== TESTE DE CLONAGEM DE DADOS ===\n\n";

try {
    // 1. Criar clientes de teste se não existirem
    echo "1. Verificando/criando clientes de teste...\n";
    
    $sourceClient = Client::firstOrCreate([
        'database_name' => 'cliente_origem_test'
    ], [
        'name' => 'Cliente Origem (Teste)',
        'slug' => 'cliente-origem-test',
        'description' => 'Cliente de origem para teste de clonagem'
    ]);
    
    $targetClient = Client::firstOrCreate([
        'database_name' => 'cliente_destino_test'
    ], [
        'name' => 'Cliente Destino (Teste)',
        'slug' => 'cliente-destino-test',
        'description' => 'Cliente de destino para teste de clonagem'
    ]);
    
    echo "   ✓ Cliente origem: {$sourceClient->name} (ID: {$sourceClient->id})\n";
    echo "   ✓ Cliente destino: {$targetClient->name} (ID: {$targetClient->id})\n\n";
    
    // 2. Conectar aos bancos de dados dos clientes
    echo "2. Conectando aos bancos de dados...\n";
    
    $sourceClient->connectToDatabase('source');
    $targetClient->connectToDatabase('target');
    
    echo "   ✓ Conexão com banco de origem estabelecida\n";
    echo "   ✓ Conexão com banco de destino estabelecida\n\n";
    
    // 3. Criar tabela de exemplo no cliente de origem
    echo "3. Criando dados de exemplo no cliente de origem...\n";
    
    // Criar tabela de produtos se não existir
    if (!Schema::connection('source')->hasTable('produtos')) {
        Schema::connection('source')->create('produtos', function ($table) {
            $table->id();
            $table->string('nome');
            $table->decimal('preco', 10, 2);
            $table->text('descricao')->nullable();
            $table->integer('estoque')->default(0);
            $table->boolean('ativo')->default(true);
            $table->timestamps();
        });
        
        echo "   ✓ Tabela 'produtos' criada\n";
    }
    
    // Inserir dados de exemplo
    $produtos = [
        ['nome' => 'Notebook Dell', 'preco' => 2500.00, 'descricao' => 'Notebook para trabalho', 'estoque' => 10],
        ['nome' => 'Mouse Logitech', 'preco' => 89.90, 'descricao' => 'Mouse sem fio', 'estoque' => 50],
        ['nome' => 'Teclado Mecânico', 'preco' => 299.99, 'descricao' => 'Teclado para gamers', 'estoque' => 25],
        ['nome' => 'Monitor 24"', 'preco' => 899.00, 'descricao' => 'Monitor Full HD', 'estoque' => 15],
        ['nome' => 'Webcam HD', 'preco' => 199.90, 'descricao' => 'Webcam para videoconferências', 'estoque' => 30],
    ];
    
    foreach ($produtos as $produto) {
        DB::connection('source')->table('produtos')->updateOrInsert(
            ['nome' => $produto['nome']],
            array_merge($produto, [
                'created_at' => now(),
                'updated_at' => now()
            ])
        );
    }
    
    $totalProdutos = DB::connection('source')->table('produtos')->count();
    echo "   ✓ {$totalProdutos} produtos inseridos na tabela de origem\n\n";
    
    // 4. Criar tabela de categorias
    if (!Schema::connection('source')->hasTable('categorias')) {
        Schema::connection('source')->create('categorias', function ($table) {
            $table->id();
            $table->string('nome');
            $table->string('slug');
            $table->text('descricao')->nullable();
            $table->timestamps();
        });
        
        echo "   ✓ Tabela 'categorias' criada\n";
    }
    
    $categorias = [
        ['nome' => 'Eletrônicos', 'slug' => 'eletronicos', 'descricao' => 'Produtos eletrônicos em geral'],
        ['nome' => 'Informática', 'slug' => 'informatica', 'descricao' => 'Produtos de informática'],
        ['nome' => 'Periféricos', 'slug' => 'perifericos', 'descricao' => 'Periféricos para computador'],
    ];
    
    foreach ($categorias as $categoria) {
        DB::connection('source')->table('categorias')->updateOrInsert(
            ['slug' => $categoria['slug']],
            array_merge($categoria, [
                'created_at' => now(),
                'updated_at' => now()
            ])
        );
    }
    
    $totalCategorias = DB::connection('source')->table('categorias')->count();
    echo "   ✓ {$totalCategorias} categorias inseridas na tabela de origem\n\n";
    
    // 5. Simular chamada da API de clonagem
    echo "4. Simulando clonagem via API...\n";
    
    $cloneData = [
        'source_client' => $sourceClient->id,
        'target_client' => $targetClient->id,
        'tables' => ['produtos', 'categorias'],
        'include_structure' => true,
        'include_data' => true,
        'overwrite' => true
    ];
    
    echo "   Dados da clonagem:\n";
    echo "   - Cliente origem: {$sourceClient->database_name}\n";
    echo "   - Cliente destino: {$targetClient->database_name}\n";
    echo "   - Tabelas: " . implode(', ', $cloneData['tables']) . "\n";
    echo "   - Incluir estrutura: " . ($cloneData['include_structure'] ? 'Sim' : 'Não') . "\n";
    echo "   - Incluir dados: " . ($cloneData['include_data'] ? 'Sim' : 'Não') . "\n";
    echo "   - Sobrescrever: " . ($cloneData['overwrite'] ? 'Sim' : 'Não') . "\n\n";
    
    // Instanciar o controller e executar a clonagem
    $controller = new \App\Http\Controllers\Api\CloneController();
    $request = new \Illuminate\Http\Request();
    $request->merge($cloneData);
    
    echo "5. Executando clonagem...\n";
    $response = $controller->clone($request);
    $result = $response->getData(true);
    
    if ($result['success']) {
        echo "   ✅ CLONAGEM CONCLUÍDA COM SUCESSO!\n\n";
        echo "   Resumo: {$result['summary']}\n";
        
        if (isset($result['statistics'])) {
            $stats = $result['statistics'];
            echo "   Estatísticas:\n";
            echo "   - Tabelas clonadas: {$stats['cloned_tables']}\n";
            echo "   - Total de linhas: {$stats['total_rows']}\n";
            echo "   - Tempo de processamento: {$stats['processing_time']}s\n";
            echo "   - Erros: {$stats['errors']}\n\n";
        }
        
        if (isset($result['details'])) {
            echo "   Detalhes:\n";
            foreach ($result['details'] as $detail) {
                echo "   {$detail}\n";
            }
        }
        
        // 6. Verificar dados clonados
        echo "\n6. Verificando dados clonados no destino...\n";
        
        foreach ($cloneData['tables'] as $table) {
            if (Schema::connection('target')->hasTable($table)) {
                $count = DB::connection('target')->table($table)->count();
                echo "   ✓ Tabela '{$table}': {$count} registros\n";
            } else {
                echo "   ❌ Tabela '{$table}': não encontrada\n";
            }
        }
        
    } else {
        echo "   ❌ ERRO NA CLONAGEM: {$result['message']}\n";
    }
    
} catch (Exception $e) {
    echo "❌ ERRO: " . $e->getMessage() . "\n";
    echo "Trace: " . $e->getTraceAsString() . "\n";
}

echo "\n=== TESTE CONCLUÍDO ===\n";