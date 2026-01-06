/**
 * Configurações do sistema WhatsApp + IA + Supabase
 */

const dotenv = require('dotenv');
dotenv.config();

const config = {
    // Ambiente
    environment: process.env.NODE_ENV || 'development',
    
    // Servidor
    server: {
        port: parseInt(process.env.PORT) || 8080,
        host: process.env.HOST || 'localhost'
    },

    // OpenAI
    openai: {
        apiKey: process.env.OPENAI_API_KEY,
        model: process.env.OPENAI_MODEL || 'gpt-4o',
        temperature: parseFloat(process.env.OPENAI_TEMPERATURE) || 0.7,
        maxTokens: parseInt(process.env.OPENAI_MAX_TOKENS) || 1000
    },

    // Anthropic (alternativo)
    anthropic: {
        apiKey: process.env.ANTHROPIC_API_KEY,
        model: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514',
        temperature: parseFloat(process.env.ANTHROPIC_TEMPERATURE) || 0.7,
        maxTokens: parseInt(process.env.ANTHROPIC_MAX_TOKENS) || 1000
    },

    // Supabase
    supabase: {
        url: process.env.SUPABASE_URL,
        key: process.env.SUPABASE_KEY,
        schema: 'public'
    },

    // WhatsApp
    whatsapp: {
        enabled: process.env.WHATSAPP_ENABLED === 'true',
        sessionPath: process.env.WHATSAPP_SESSION_PATH || './whatsapp-session-unified',
        clientId: process.env.WHATSAPP_CLIENT_ID || 'unified-ai-assistant',
        puppeteerOptions: {
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--single-process',
                '--disable-gpu'
            ]
        }
    },

    // Formatação de respostas
    formatting: {
        maxMessageLength: 4000,
        maxRecordsToShow: 20,
        dateFormat: 'pt-BR',
        currency: 'BRL'
    },

    // Limites e timeouts
    limits: {
        maxQueryResults: 100,
        queryTimeout: 30000, // 30 segundos
        aiTimeout: 15000,    // 15 segundos
        retryAttempts: 3,
        retryDelay: 1000
    },

    // Logs
    logging: {
        level: process.env.LOG_LEVEL || 'info',
        providerEvents: process.env.LOG_PROVIDER_EVENTS === 'true',
        enableHealthChecks: process.env.ENABLE_HEALTH_CHECKS === 'true',
        healthCheckInterval: parseInt(process.env.HEALTH_CHECK_INTERVAL) || 30000
    },

    // Providers
    providers: {
        autoInitialize: process.env.AUTO_INITIALIZE_PROVIDERS === 'true',
        retryAttempts: parseInt(process.env.PROVIDER_RETRY_ATTEMPTS) || 3,
        retryDelay: parseInt(process.env.PROVIDER_RETRY_DELAY) || 1000
    },

    // Uploads
    uploads: {
        path: process.env.UPLOAD_PATH || './uploads',
        maxSize: 10 * 1024 * 1024, // 10MB
        allowedTypes: ['image/jpeg', 'image/png', 'image/gif', 'application/pdf']
    },

    // Google (se necessário)
    google: {
        clientId: process.env.GOOGLE_CLIENT_ID,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET,
        driveRedirectUri: process.env.GOOGLE_DRIVE_REDIRECT_URI,
        gmailRedirectUri: process.env.GMAIL_REDIRECT_URI
    }
};

// Validações
function validateConfig() {
    const errors = [];

    // Validações obrigatórias
    if (!config.openai.apiKey) {
        errors.push('OPENAI_API_KEY é obrigatório');
    }

    if (!config.supabase.url || !config.supabase.key) {
        errors.push('SUPABASE_URL e SUPABASE_KEY são obrigatórios');
    }

    if (config.whatsapp.enabled && !config.whatsapp.sessionPath) {
        errors.push('WHATSAPP_SESSION_PATH é obrigatório quando WhatsApp está habilitado');
    }

    if (errors.length > 0) {
        console.error('❌ Erros de configuração:');
        errors.forEach(error => console.error(`  - ${error}`));
        throw new Error('Configuração inválida');
    }

    console.log('✅ Configuração validada com sucesso');
}

// Função para obter configuração por ambiente
function getEnvironmentConfig() {
    const envConfigs = {
        development: {
            logging: { level: 'debug' },
            limits: { queryTimeout: 60000 }
        },
        production: {
            logging: { level: 'info' },
            limits: { queryTimeout: 30000 },
            whatsapp: {
                puppeteerOptions: {
                    ...config.whatsapp.puppeteerOptions,
                    headless: true
                }
            }
        },
        test: {
            logging: { level: 'error' },
            whatsapp: { enabled: false }
        }
    };

    const envConfig = envConfigs[config.environment] || {};
    
    // Merge configurações
    return mergeDeep(config, envConfig);
}

// Função utilitária para merge profundo
function mergeDeep(target, source) {
    const output = Object.assign({}, target);
    
    if (isObject(target) && isObject(source)) {
        Object.keys(source).forEach(key => {
            if (isObject(source[key])) {
                if (!(key in target)) {
                    Object.assign(output, { [key]: source[key] });
                } else {
                    output[key] = mergeDeep(target[key], source[key]);
                }
            } else {
                Object.assign(output, { [key]: source[key] });
            }
        });
    }
    
    return output;
}

function isObject(item) {
    return item && typeof item === 'object' && !Array.isArray(item);
}

// Exporta configuração final
const finalConfig = getEnvironmentConfig();

// Valida apenas se não estiver em modo de teste
if (process.env.NODE_ENV !== 'test') {
    validateConfig();
}

module.exports = finalConfig;