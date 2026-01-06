const express = require('express');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const dotenv = require('dotenv');
const WhatsAppBot = require('./whatsapp/bot');
const MultiAgentSystem = require('./ai/multiagent-system');
const SupabaseExecutor = require('./supabase/executor');
const ResponseFormatter = require('./formatters/response');

// Carrega variáveis de ambiente
dotenv.config();

// Previne múltiplas inicializações
if (global.systemRunning) {
    console.log('⚠️ Sistema já está rodando!');
    process.exit(0);
}
global.systemRunning = true;

// Limpa handlers anteriores
process.removeAllListeners('SIGINT');
process.removeAllListeners('SIGTERM');

class WhatsAppAISystem {
    constructor() {
        this.app = express();
        this.port = process.env.PORT || 8080;
        this.whatsappBot = null;
        this.aiAgent = null;
        this.supabaseExecutor = null;
        this.responseFormatter = null;
        
        this.setupMiddleware();
        this.setupRoutes();
    }

    setupMiddleware() {
        this.app.use(express.json());
        this.app.use(express.urlencoded({ extended: true }));
    }

    setupRoutes() {
        this.app.get('/', (req, res) => {
            res.json({
                status: 'running',
                message: 'WhatsApp + IA + Supabase System',
                timestamp: new Date().toISOString()
            });
        });

        this.app.get('/health', (req, res) => {
            res.json({
                status: 'healthy',
                whatsapp: this.whatsappBot?.isReady || false,
                supabase: this.supabaseExecutor?.isConnected || false,
                ai: this.aiAgent?.isReady || false
            });
        });
    }

    async initialize() {
        try {
            console.log('🚀 Inicializando sistema WhatsApp + IA + Supabase...');
            console.log('📋 Verificando variáveis de ambiente...');

            // Verifica variáveis essenciais
            const required = ['OPENAI_API_KEY', 'SUPABASE_URL', 'SUPABASE_KEY'];
            const missing = required.filter(v => !process.env[v]);
            
            if (missing.length > 0) {
                throw new Error(`Variáveis obrigatórias não configuradas: ${missing.join(', ')}`);
            }
            console.log('✅ Variáveis de ambiente OK');

            // Inicializa componentes
            console.log('🔧 Inicializando componentes...');
            this.responseFormatter = new ResponseFormatter();
            console.log('✅ ResponseFormatter inicializado');
            
            this.supabaseExecutor = new SupabaseExecutor();
            console.log('✅ SupabaseExecutor inicializado');
            
            this.aiAgent = new MultiAgentSystem(this.supabaseExecutor);
            console.log('✅ MultiAgentSystem inicializado');
            
            this.whatsappBot = new WhatsAppBot(this.aiAgent, this.responseFormatter);
            console.log('✅ WhatsAppBot inicializado');

            // Testa conexão com Supabase
            console.log('🔗 Testando conexão com Supabase...');
            await this.supabaseExecutor.testConnection();
            console.log('✅ Conexão com Supabase estabelecida');

            // Inicializa WhatsApp Bot
            console.log('📱 Inicializando WhatsApp Bot...');
            await this.whatsappBot.initialize();
            console.log('✅ WhatsApp Bot inicializado');

            // Inicia servidor Express
            this.app.listen(this.port, () => {
                console.log(`🌐 Servidor rodando na porta ${this.port}`);
                console.log(`📱 Sistema pronto para receber mensagens!`);
                console.log(`🔗 Status: http://localhost:${this.port}/health`);
            });

        } catch (error) {
            console.error('❌ Erro ao inicializar sistema:', error);
            console.error('Stack trace:', error.stack);
            global.systemRunning = false;
            process.exit(1);
        }
    }

    async shutdown() {
        console.log('🛑 Encerrando sistema...');
        
        try {
            if (this.whatsappBot) {
                console.log('📱 Encerrando WhatsApp Bot...');
                await this.whatsappBot.destroy();
            }
            
            global.systemRunning = false;
            console.log('✅ Sistema encerrado com sucesso');
        } catch (error) {
            console.error('❌ Erro ao encerrar sistema:', error);
        }
        
        process.exit(0);
    }
}

// Inicializa o sistema
const system = new WhatsAppAISystem();

// Handlers para encerramento gracioso
process.on('SIGINT', () => system.shutdown());
process.on('SIGTERM', () => system.shutdown());

// Inicia o sistema
system.initialize().catch(console.error);

module.exports = WhatsAppAISystem;