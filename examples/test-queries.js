/**
 * Exemplos e testes para o sistema WhatsApp + IA + Supabase
 * Execute este arquivo para testar as funcionalidades sem WhatsApp
 */

const dotenv = require('dotenv');
const AIAgent = require('../src/ai/agent');
const SupabaseExecutor = require('../src/supabase/executor');
const ResponseFormatter = require('../src/formatters/response');

// Carrega variáveis de ambiente
dotenv.config();

class SystemTester {
    constructor() {
        this.supabaseExecutor = new SupabaseExecutor();
        this.aiAgent = new AIAgent(this.supabaseExecutor);
        this.responseFormatter = new ResponseFormatter();
    }

    async initialize() {
        console.log('🚀 Inicializando testes do sistema...\n');
        
        try {
            // Testa conexão Supabase
            await this.supabaseExecutor.testConnection();
            console.log('✅ Conexão Supabase OK\n');
            
            // Testa IA
            const aiTest = await this.aiAgent.testAI();
            console.log(`✅ Conexão OpenAI ${aiTest ? 'OK' : 'FALHOU'}\n`);
            
            return true;
        } catch (error) {
            console.error('❌ Erro na inicialização:', error.message);
            return false;
        }
    }

    async runTests() {
        const testQueries = [
            "quantas tabelas tem?",
            "quantos leads tem?",
            "mostra os últimos 5 clientes",
            "clientes com email gmail",
            "produtos com preço maior que 100",
            "busca cliente João"
        ];

        console.log('🧪 Executando testes de queries...\n');

        for (const query of testQueries) {
            await this.testQuery(query);
            console.log('─'.repeat(50));
        }
    }

    async testQuery(query) {
        try {
            console.log(`📝 Testando: "${query}"`);
            
            const userContext = {
                name: 'Teste',
                number: '5511999999999',
                timestamp: new Date().toISOString()
            };

            // Processa com IA
            const aiResponse = await this.aiAgent.processMessage(query, userContext);
            
            // Formata resposta
            const formattedResponse = this.responseFormatter.format(aiResponse);
            
            console.log('🤖 Resposta:');
            console.log(formattedResponse);
            console.log();

        } catch (error) {
            console.error(`❌ Erro ao testar "${query}":`, error.message);
            console.log();
        }
    }

    async testSupabaseDirectly() {
        console.log('🗄️ Testando Supabase diretamente...\n');

        try {
            // Lista tabelas
            console.log('📊 Listando tabelas:');
            const tables = await this.supabaseExecutor.listTables();
            console.log(JSON.stringify(tables, null, 2));
            console.log();

            // Se houver tabelas, testa operações
            if (tables.success && tables.data.length > 0) {
                const firstTable = tables.data[0].table_name;
                
                // Conta registros
                console.log(`🔢 Contando registros da tabela "${firstTable}":`);
                const count = await this.supabaseExecutor.countRecords(firstTable);
                console.log(JSON.stringify(count, null, 2));
                console.log();

                // Lista alguns registros
                console.log(`📋 Listando registros da tabela "${firstTable}":`);
                const records = await this.supabaseExecutor.listRecords(firstTable, { limit: 3 });
                console.log(JSON.stringify(records, null, 2));
                console.log();
            }

        } catch (error) {
            console.error('❌ Erro no teste direto Supabase:', error.message);
        }
    }

    async demonstrateFormatting() {
        console.log('🎨 Demonstrando formatação de respostas...\n');

        // Exemplo de resposta de contagem
        const countResponse = {
            intent: {
                operation: 'count',
                table: 'clientes',
                explanation: 'Vou contar quantos clientes existem'
            },
            result: {
                success: true,
                data: { count: 150 },
                message: 'Contagem realizada com sucesso'
            },
            userContext: { name: 'Teste' }
        };

        console.log('📊 Exemplo - Contagem:');
        console.log(this.responseFormatter.format(countResponse));
        console.log('─'.repeat(30));

        // Exemplo de resposta de listagem
        const listResponse = {
            intent: {
                operation: 'list',
                table: 'produtos',
                limit: 3,
                explanation: 'Vou mostrar os produtos disponíveis'
            },
            result: {
                success: true,
                data: [
                    { id: 1, nome: 'Notebook Dell', preco: 2500.00, categoria: 'Eletrônicos' },
                    { id: 2, nome: 'Mouse Logitech', preco: 89.90, categoria: 'Acessórios' },
                    { id: 3, nome: 'Teclado Mecânico', preco: 299.99, categoria: 'Acessórios' }
                ],
                count: 3,
                message: 'Produtos encontrados'
            },
            userContext: { name: 'Teste' }
        };

        console.log('📋 Exemplo - Listagem:');
        console.log(this.responseFormatter.format(listResponse));
        console.log('─'.repeat(30));

        // Exemplo de erro
        const errorResponse = {
            intent: {
                operation: 'list',
                table: 'tabela_inexistente',
                explanation: 'Tentando listar tabela inexistente'
            },
            result: {
                success: false,
                error: 'relation "tabela_inexistente" does not exist',
                message: 'Erro ao executar query'
            },
            userContext: { name: 'Teste' }
        };

        console.log('❌ Exemplo - Erro:');
        console.log(this.responseFormatter.format(errorResponse));
        console.log();
    }
}

// Função principal para executar os testes
async function main() {
    const tester = new SystemTester();
    
    // Inicializa sistema
    const initialized = await tester.initialize();
    if (!initialized) {
        console.log('❌ Falha na inicialização. Verifique as configurações no .env');
        process.exit(1);
    }

    // Menu de opções
    const args = process.argv.slice(2);
    const option = args[0] || 'all';

    switch (option) {
        case 'queries':
            await tester.runTests();
            break;
            
        case 'supabase':
            await tester.testSupabaseDirectly();
            break;
            
        case 'format':
            await tester.demonstrateFormatting();
            break;
            
        case 'all':
        default:
            await tester.testSupabaseDirectly();
            console.log('\n' + '='.repeat(50) + '\n');
            await tester.demonstrateFormatting();
            console.log('\n' + '='.repeat(50) + '\n');
            await tester.runTests();
            break;
    }

    console.log('✅ Testes concluídos!');
    process.exit(0);
}

// Executa se chamado diretamente
if (require.main === module) {
    main().catch(error => {
        console.error('❌ Erro nos testes:', error);
        process.exit(1);
    });
}

module.exports = SystemTester;