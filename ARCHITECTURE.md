# 📚 Documentação Técnica - AI Data Scientist AICODEPRO
## Sistema de BI Conversacional via WhatsApp com Multiagentes

### 🏗️ Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                    SISTEMA MULTIAGENTES                        │
│                                                                 │
│  ⚡ FAST PATH (META_PATTERNS → SYSTEM_IDENTITY → <50ms)        │
│  ─────────────────────────────────────────────────────────────  │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │ Coordenador │  │   Schema    │  │    Query    │            │
│  │   Agent     │→ │   Agent     │→ │   Agent     │            │
│  └─────────────┘  └─────────────┘  └─────────────┘            │
│         ↓                 ↓                 ↓                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │  Analyst    │  │ Formatter   │  │   MCP       │            │
│  │   Agent     │  │   Agent     │  │  Server     │            │
│  └─────────────┘  └─────────────┘  └─────────────┘            │
└─────────────────────────────────────────────────────────────────┘
         ↑                                           ↓
┌─────────────────┐                         ┌─────────────────┐
│   WhatsApp      │                         │   Supabase      │
│     Bot         │                         │   Database      │
│  (Interface)    │                         │   (Storage)     │
└─────────────────┘                         └─────────────────┘
```

### ⚡ Fast Path - Respostas Instantâneas

O sistema implementa um **Fast Path** que detecta perguntas meta/conversacionais e responde instantaneamente (<50ms) sem consultar o banco de dados:

```
Mensagem → META_PATTERNS (regex) → SYSTEM_IDENTITY → Resposta Instantânea
                    ↓ (não match)
              Full Path (5 agentes)
```

**Perguntas detectadas:**
- "O que você pode fazer?" / "Quais suas capacidades?"
- "Oi" / "Olá" / "Bom dia" (saudações)
- "Obrigado" / "Valeu" (agradecimentos)
- "Status" / "Teste" / "Ping"

**SYSTEM_IDENTITY contém:**
- Nome: "AI Data Scientist - AICODEPRO"
- Role: "Cientista de Dados & Business Intelligence"
- Capacidades: Ciência de Dados, BI, Análise Educacional, Queries Avançadas
- Tabelas conhecidas: 8 tabelas documentadas

---

## 📁 ESTRUTURA DE PASTAS E ARQUIVOS

### **📂 `/` (Raiz do Projeto)**

#### **📄 `package.json`**
```json
{
  "name": "whatsapp-ia-supabase",
  "description": "Sistema multiagentes para análise de dados via WhatsApp",
  "dependencies": {
    "@anthropic-ai/sdk": "Claude 4.5 para IA",
    "@supabase/supabase-js": "Cliente Supabase",
    "whatsapp-web.js": "Interface WhatsApp",
    "express": "Servidor web"
  }
}
```
**Propósito**: Define dependências e metadados do projeto

#### **📄 `start.js`**
```javascript
// Ponto de entrada principal
require('./src/index.js');
```
**Propósito**: Script de inicialização que carrega o sistema principal

#### **📄 `.env`**
```env
# Configurações sensíveis
ANTHROPIC_API_KEY=sk-ant-api03-...
SUPABASE_URL=https://projeto.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIs...
```
**Propósito**: Variáveis de ambiente para APIs e configurações

#### **📄 `README.md`**
**Propósito**: Documentação principal para usuários e desenvolvedores

#### **📄 `EXAMPLES.md`**
**Propósito**: Casos de uso práticos e exemplos de funcionamento

---

### **📂 `/config` - Configurações do Sistema**

#### **📄 `config.js`**
```javascript
module.exports = {
    supabase: {
        url: process.env.SUPABASE_URL,
        key: process.env.SUPABASE_KEY
    },
    anthropic: {
        apiKey: process.env.ANTHROPIC_API_KEY,
        model: 'claude-sonnet-4-20250514',
        maxTokens: 1500
    },
    whatsapp: {
        sessionPath: './whatsapp-session-unified',
        clientId: 'unified-ai-assistant'
    }
};
```
**Propósito**: Centraliza todas as configurações do sistema

---

### **📂 `/src` - Código Fonte Principal**

#### **📄 `index.js` - Orquestrador Principal**
```javascript
class WhatsAppAISystem {
    constructor() {
        this.whatsappBot = null;
        this.aiAgent = null; // Sistema Multiagentes
        this.supabaseExecutor = null;
    }
    
    async initialize() {
        // 1. Inicializa Supabase
        // 2. Inicializa Sistema Multiagentes
        // 3. Inicializa WhatsApp Bot
        // 4. Inicia servidor Express
    }
}
```
**Propósito**: 
- Orquestra inicialização de todos os componentes
- Gerencia ciclo de vida do sistema
- Configura servidor Express para monitoramento

---

### **📂 `/src/ai` - Sistema de Inteligência Artificial**

#### **📄 `multiagent-system.js` - Cérebro do Sistema**
```javascript
class MultiAgentSystem {
    constructor(supabaseExecutor) {
        this.supabaseExecutor = supabaseExecutor;
        this.anthropic = new Anthropic();
        this.schemaCache = new Map(); // Cache inteligente
    }
    
    async processMessage(messageText, userContext) {
        // 1. Coordenador analisa intenção
        const intention = await this.coordinatorAgent(messageText);
        
        // 2. Schema descobre estrutura
        const schema = await this.schemaAgent(intention.tables_needed);
        
        // 3. Query constrói e executa SQL
        const queryResult = await this.queryAgent(intention, schema);
        
        // 4. Analyst gera insights
        const analysis = await this.analystAgent(queryResult, intention);
        
        // 5. Formatter cria resposta WhatsApp
        const response = await this.formatterAgent(analysis, queryResult);
        
        return response;
    }
}
```

**Agentes Especializados:**

##### **🤖 Agente Coordenador**
```javascript
async coordinatorAgent(messageText) {
    // Analisa intenção do usuário
    // Determina tabelas necessárias
    // Planeja estratégia de execução
    // Retorna: { analysis_type, tables_needed, operations, complexity }
}
```
**Propósito**: Interpreta linguagem natural e planeja execução

##### **🗄️ Agente Schema**
```javascript
async schemaAgent(tablesNeeded) {
    // Descobre estrutura das tabelas dinamicamente
    // Obtém colunas via RPC list_columns
    // Mantém cache para performance
    // Retorna: [{ table_name, columns, row_count, sample_data }]
}
```
**Propósito**: Descoberta automática de metadados do banco

##### **🔍 Agente Query**
```javascript
async queryAgent(intention, schemas) {
    // Gera SQL inteligente baseado na intenção
    // Detecta COUNT DISTINCT, agregações, filtros
    // Executa com método otimizado
    // Retorna: { sql_strategy, results }
}
```
**Propósito**: Construção e execução de SQL inteligente

##### **📊 Agente Analyst**
```javascript
async analystAgent(queryResult, intention) {
    // Analisa resultados dos dados
    // Gera insights de negócio
    // Identifica padrões e tendências
    // Retorna: { insights, summary, recommendations }
}
```
**Propósito**: Análise inteligente e geração de insights

##### **💬 Agente Formatter**
```javascript
async formatterAgent(analysis, queryResult) {
    // Formata resposta para WhatsApp
    // Adiciona emojis e estrutura
    // Otimiza para mobile
    // Retorna: string formatada
}
```
**Propósito**: Formatação otimizada para WhatsApp

#### **📄 `agent.js` - Agente IA Legacy**
```javascript
class AIAgent {
    // Sistema anterior baseado em MCP
    // Mantido para compatibilidade
    // Será depreciado em favor do multiagent-system
}
```
**Propósito**: Sistema legado, mantido para referência

---

### **📂 `/src/supabase` - Camada de Dados**

#### **📄 `executor.js` - Executor de Queries Supabase**
```javascript
class SupabaseExecutor {
    constructor() {
        this.supabase = createClient(url, key);
        this.isConnected = false;
    }
    
    // === DESCOBERTA AUTOMÁTICA ===
    async getTablesInfo() {
        // 1. Tenta RPC list_tables
        // 2. Fallback: fetch direto
        // 3. Fallback: tabelas conhecidas
    }
    
    async enrichTablesWithColumns(tables) {
        // Obtém estrutura via RPC list_columns
        // Fallback: amostra de dados
    }
    
    // === OPERAÇÕES DE DADOS ===
    async countRecords(tableName, filters) {
        // Contagem simples com filtros
    }
    
    async listRecords(tableName, options) {
        // Listagem com paginação, filtros, ordenação
    }
    
    async performAggregation(tableName, aggregation) {
        // COUNT DISTINCT com paginação automática
        // Busca TODOS os registros em páginas de 1000
    }
    
    async getAllRecordsWithPagination(tableName, column) {
        // Paginação inteligente para grandes datasets
        // Resolve problema dos 1000 registros limite
    }
}
```

**Métodos Principais:**

##### **🔍 Descoberta Automática**
```javascript
async getTablesInfo() {
    // Método 1: RPC list_tables (automático)
    // Método 2: Fetch direto (fallback)
    // Método 3: Teste manual (último recurso)
}
```

##### **📊 Agregações Inteligentes**
```javascript
async performAggregation(tableName, aggregation) {
    if (aggregation.type === 'count_distinct') {
        // Usa paginação para buscar TODOS os registros
        const allData = await this.getAllRecordsWithPagination();
        const uniqueValues = new Set(allData.map(row => row[column]));
        return uniqueValues.size; // Contagem precisa
    }
}
```

**Propósito**: 
- Interface única para todas as operações Supabase
- Descoberta automática de estruturas
- Otimização de performance com cache
- Tratamento de limitações da API

---

### **📂 `/src/whatsapp` - Interface WhatsApp**

#### **📄 `bot.js` - Bot WhatsApp**
```javascript
class WhatsAppBot {
    constructor(aiAgent, responseFormatter) {
        this.aiAgent = aiAgent; // Sistema Multiagentes
        this.client = new Client({ authStrategy: LocalAuth });
    }
    
    async handleMessage(message) {
        // 1. Valida mensagem
        // 2. Mostra "digitando..."
        // 3. Processa com Sistema Multiagentes
        // 4. Envia resposta formatada
    }
    
    async processMessageWithAI(messageText, contact) {
        // Chama sistema multiagentes
        const aiResponse = await this.aiAgent.processMessage(messageText);
        return this.responseFormatter.format(aiResponse);
    }
}
```

**Funcionalidades:**
- **Autenticação**: LocalAuth para sessão persistente
- **QR Code**: Geração automática para login
- **Processamento**: Integração com sistema multiagentes
- **Formatação**: Respostas otimizadas para WhatsApp

**Propósito**: Interface entre WhatsApp e sistema de IA

---

### **📂 `/src/formatters` - Formatação de Respostas**

#### **📄 `response.js` - Formatador de Respostas**
```javascript
class ResponseFormatter {
    format(aiResponse) {
        // Adiciona emojis apropriados
        // Estrutura com títulos e listas
        // Destaca números importantes
        // Otimiza para leitura mobile
    }
    
    formatTableData(data) {
        // Converte dados tabulares em texto
    }
    
    formatInsights(insights) {
        // Formata insights de negócio
    }
}
```
**Propósito**: Converte dados técnicos em mensagens amigáveis

---

### **📂 `/src/mcp` - Model Context Protocol**

#### **📄 `supabase-server.js` - Servidor MCP**
```javascript
class SupabaseMCPServer {
    // Implementação futura do protocolo MCP
    // Para integração com outros sistemas
    // Atualmente não utilizado
}
```
**Propósito**: Preparação para protocolo MCP (futuro)

---

### **📂 `/src/utils` - Utilitários**

#### **📄 `helpers.js` - Funções Auxiliares**
```javascript
function validateConfig() {
    // Valida variáveis de ambiente
}

function formatNumber(num) {
    // Formatação de números para PT-BR
}

function sanitizeInput(input) {
    // Sanitização de entradas
}
```
**Propósito**: Funções utilitárias compartilhadas

---

### **📂 `/examples` - Exemplos e Testes**

#### **📄 `test-queries.js` - Testes de Queries**
```javascript
// Exemplos de queries para teste
// Casos de uso documentados
// Validação de funcionalidades
```
**Propósito**: Testes e exemplos de uso

---

### **📂 `/scripts` - Scripts de Automação**
```bash
# Scripts para deploy, backup, manutenção
```
**Propósito**: Automação de tarefas operacionais

---

## 🔄 FLUXO DE DADOS COMPLETO

### **1. Recepção da Mensagem**
```
WhatsApp → bot.js → handleMessage()
```

### **2. Processamento Multiagentes**
```
multiagent-system.js:
├── coordinatorAgent() → Analisa intenção
├── schemaAgent() → Descobre estrutura  
├── queryAgent() → Gera e executa SQL
├── analystAgent() → Gera insights
└── formatterAgent() → Formata resposta
```

### **3. Execução de Dados**
```
executor.js:
├── getTablesInfo() → Descoberta automática
├── performAggregation() → COUNT DISTINCT preciso
└── getAllRecordsWithPagination() → Busca completa
```

### **4. Resposta ao Usuário**
```
response.js → bot.js → WhatsApp
```

---

## 🎯 PONTOS CRÍTICOS DE ARQUITETURA

### **Cache Inteligente**
- **Schema Cache**: 5 minutos de TTL
- **Metadados**: Evita descoberta repetitiva
- **Performance**: Reduz latência significativamente

### **Paginação Automática**
- **Problema**: Supabase limita 1000 registros/query
- **Solução**: Paginação transparente em `getAllRecordsWithPagination()`
- **Resultado**: COUNT DISTINCT preciso para qualquer volume

### **Descoberta Dinâmica**
- **Zero Hardcode**: Tudo descoberto via RPC
- **Adaptabilidade**: Funciona com qualquer schema
- **Robustez**: Múltiplos fallbacks

### **Sistema Multiagentes**
- **Especialização**: Cada agente tem responsabilidade única
- **Coordenação**: Fluxo orquestrado pelo processMessage()
- **Inteligência**: Claude 4 raciocina em cada etapa

---

## 🔧 CONFIGURAÇÕES CRÍTICAS

### **Supabase RPC Functions**
```sql
-- Necessárias para descoberta automática
CREATE FUNCTION list_tables() RETURNS TABLE(table_name text);
CREATE FUNCTION list_columns(table_name text) RETURNS TABLE(column_name text);
```

### **Variáveis de Ambiente**
```env
ANTHROPIC_API_KEY=    # Claude 4.5 API
SUPABASE_URL=         # URL do projeto
SUPABASE_KEY=         # Service role key
```

### **Dependências Críticas**
- **@anthropic-ai/sdk**: IA principal
- **@supabase/supabase-js**: Cliente banco
- **whatsapp-web.js**: Interface WhatsApp

---

**Esta arquitetura garante escalabilidade, manutenibilidade e precisão nas análises de dados via WhatsApp.**