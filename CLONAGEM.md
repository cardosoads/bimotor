# Funcionalidade de Clonagem de Dados

## Visão Geral

A aplicação Laravel Bimotor agora inclui uma funcionalidade completa de clonagem de dados entre clientes, compatibilizada com as aplicações de clonagem existentes (db-monitor-clean, db-monitor-lite, db-monitor-builder).

## Características

### ✅ Funcionalidades Implementadas

- **Clonagem de Estrutura**: Copia a estrutura das tabelas (colunas, tipos de dados)
- **Clonagem de Dados**: Copia todos os dados das tabelas selecionadas
- **Clonagem Seletiva**: Permite escolher quais tabelas clonar
- **Sobrescrita Controlada**: Opção para sobrescrever tabelas existentes
- **Suporte Multi-Tenant**: Funciona com o sistema de clientes isolados
- **Interface Web**: Interface moderna em Vue.js + TypeScript
- **API RESTful**: Endpoint para integração programática
- **Logs Detalhados**: Rastreamento completo das operações
- **Tratamento de Erros**: Rollback automático em caso de falha
- **Compatibilidade**: Funciona com MySQL e SQLite

### 🔧 Componentes Integrados

1. **CloneController** (`app/Http/Controllers/Api/CloneController.php`)
   - Controlador principal da API de clonagem
   - Validação de entrada
   - Gerenciamento de transações
   - Logs detalhados

2. **Interface Web** (`resources/js/Pages/Clone.vue`)
   - Interface moderna e responsiva
   - Formulário intuitivo
   - Exibição de resultados em tempo real
   - Feedback visual de progresso

3. **Rotas**
   - API: `POST /api/clone`
   - Web: `GET /clone`

4. **Navegação**
   - Link no menu lateral principal
   - Ícone de clonagem (Copy)

## Como Usar

### 1. Interface Web

1. Acesse a aplicação em `http://127.0.0.1:8000`
2. Faça login com suas credenciais
3. Clique em "Clonagem" no menu lateral
4. Preencha o formulário:
   - **Cliente de Origem**: ID ou nome do cliente fonte
   - **Cliente de Destino**: ID ou nome do cliente destino
   - **Tabelas**: Lista de tabelas (uma por linha)
   - **Opções**: Estrutura, dados, sobrescrever
5. Clique em "Executar Clonagem"
6. Acompanhe o progresso e resultados

### 2. API RESTful

```bash
curl -X POST http://127.0.0.1:8000/api/clone \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "source_client": "cliente_origem",
    "target_client": "cliente_destino", 
    "tables": ["produtos", "categorias"],
    "include_structure": true,
    "include_data": true,
    "overwrite": true
  }'
```

### 3. Programaticamente (PHP)

```php
use App\Http\Controllers\Api\CloneController;
use Illuminate\Http\Request;

$controller = new CloneController();
$request = new Request([
    'source_client' => 'cliente_origem',
    'target_client' => 'cliente_destino',
    'tables' => ['produtos', 'categorias'],
    'include_structure' => true,
    'include_data' => true,
    'overwrite' => true
]);

$response = $controller->clone($request);
$result = $response->getData(true);
```

## Parâmetros da API

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `source_client` | string | ✅ | ID, database_name ou slug do cliente origem |
| `target_client` | string | ✅ | ID, database_name ou slug do cliente destino |
| `tables` | array | ✅ | Lista de nomes das tabelas para clonar |
| `include_structure` | boolean | ❌ | Incluir estrutura das tabelas (padrão: true) |
| `include_data` | boolean | ❌ | Incluir dados das tabelas (padrão: true) |
| `overwrite` | boolean | ❌ | Sobrescrever tabelas existentes (padrão: false) |

## Resposta da API

### Sucesso
```json
{
  "success": true,
  "message": "Clonagem concluída",
  "summary": "2 tabelas clonadas com sucesso",
  "details": [
    "✅ Tabela 'produtos' clonada",
    "✅ Tabela 'categorias' clonada"
  ],
  "statistics": {
    "cloned_tables": 2,
    "total_rows": 8,
    "errors": 0,
    "processing_time": 0.09,
    "source_client": "cliente_origem_test",
    "target_client": "cliente_destino_test"
  }
}
```

### Erro
```json
{
  "success": false,
  "message": "Cliente não encontrado"
}
```

## Compatibilidade

### Integração com Aplicações Existentes

A funcionalidade foi desenvolvida para ser compatível com:

- **db-monitor-clean**: API similar ao `api/clone.php`
- **db-monitor-lite**: Funcionalidades documentadas em `CLONE_FEATURE.md`
- **db-monitor-builder**: Classe `DatabaseExporter` equivalente

### Diferenças e Melhorias

| Recurso | Aplicações Antigas | Laravel Bimotor |
|---------|-------------------|-----------------|
| Framework | PHP Puro | Laravel 12 |
| Interface | HTML/JS | Vue.js + TypeScript |
| Autenticação | Básica | Laravel Sanctum |
| Logs | Limitados | Logs detalhados |
| Transações | Manual | Automático |
| Validação | Básica | Laravel Validation |
| Multi-tenant | Limitado | Nativo |

## Segurança

- ✅ Autenticação obrigatória (Sanctum)
- ✅ Validação de entrada
- ✅ Proteção CSRF
- ✅ Logs de auditoria
- ✅ Transações com rollback
- ✅ Isolamento entre clientes

## Performance

- ✅ Inserção em lotes (500 registros por vez)
- ✅ Conexões otimizadas
- ✅ Logs de tempo de execução
- ✅ Gerenciamento de memória

## Teste

Execute o script de teste incluído:

```bash
php test_clone.php
```

Este script:
1. Cria clientes de teste
2. Insere dados de exemplo
3. Executa a clonagem
4. Verifica os resultados
5. Exibe estatísticas detalhadas

## Logs

Os logs da clonagem são armazenados em:
- `storage/logs/laravel.log`

Exemplo de log:
```
[2025-01-12 00:08:59] local.INFO: Requisição /clone iniciada
[2025-01-12 00:09:00] local.INFO: Iniciando clonagem da tabela: produtos
[2025-01-12 00:09:00] local.INFO: Tabela produtos clonada com sucesso
[2025-01-12 00:09:00] local.INFO: Clonagem concluída
```

## Próximos Passos

1. **Agendamento**: Implementar clonagem agendada
2. **Filtros**: Adicionar filtros de dados (WHERE)
3. **Compressão**: Otimizar transferência de grandes volumes
4. **Monitoramento**: Dashboard de operações
5. **Webhooks**: Notificações de conclusão

## Suporte

Para dúvidas ou problemas:
1. Verifique os logs em `storage/logs/laravel.log`
2. Execute o script de teste `test_clone.php`
3. Consulte a documentação da API
4. Verifique as permissões de banco de dados