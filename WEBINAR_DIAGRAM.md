# Diagrama de Relações e Fluxo - AI Data Scientist AICODEPRO

```mermaid
graph TB
    %% Arquivos principais
    start[start.js] --> index[src/index.js]
    
    %% Classe principal e componentes
    index --> WhatsAppAISystem
    
    %% Componentes principais
    WhatsAppAISystem --> WhatsAppBot[src/whatsapp/bot.js]
    WhatsAppAISystem --> MultiAgentSystem[src/ai/multiagent-system.js]
    WhatsAppAISystem --> SupabaseExecutor[src/supabase/executor.js]
    WhatsAppAISystem --> ResponseFormatter[src/formatters/response.js]
    
    %% Fluxo de inicialização
    subgraph "Fluxo de Inicialização"
        start_verify[Verificações de Ambiente] --> start_deps[Verificação de Dependências]
        start_deps --> start_system[Inicialização do Sistema]
    end
    
    start --> start_verify
    
    %% ⚡ FAST PATH - NOVO
    subgraph "⚡ Fast Path - Respostas Instantâneas"
        fp_check{META_PATTERNS?}
        fp_identity[SYSTEM_IDENTITY]
        fp_response[Resposta < 50ms]
        fp_check -->|"'oi', 'ajuda', 'o que você pode fazer?'"| fp_identity
        fp_identity --> fp_response
    end
    
    %% Fluxo de processamento de mensagens
    subgraph "Fluxo de Processamento de Mensagens (Full Path)"
        message[Mensagem do WhatsApp] --> WhatsAppBot
        WhatsAppBot --> fp_check
        fp_check -->|"Consulta de dados"| MultiAgentSystem
        MultiAgentSystem --> SupabaseExecutor
        SupabaseExecutor --> MultiAgentSystem
        MultiAgentSystem --> WhatsAppBot
        WhatsAppBot --> ResponseFormatter
        ResponseFormatter --> response[Resposta Formatada]
    end
    
    %% Dependências externas
    subgraph "Dependências Externas"
        whatsapp_web[whatsapp-web.js]
        anthropic[Anthropic API - Claude 4]
        supabase[Supabase API]
        express[Express]
        mcp[MCP Server]
    end
    
    WhatsAppBot --> whatsapp_web
    MultiAgentSystem --> anthropic
    MultiAgentSystem --> mcp
    SupabaseExecutor --> supabase
    WhatsAppAISystem --> express
    
    %% Estilo
    classDef primary fill:#f9f,stroke:#333,stroke-width:2px
    classDef secondary fill:#bbf,stroke:#333,stroke-width:1px
    classDef external fill:#bfb,stroke:#333,stroke-width:1px
    classDef fastpath fill:#90EE90,stroke:#333,stroke-width:2px
    
    class start,index primary
    class WhatsAppBot,MultiAgentSystem,SupabaseExecutor,ResponseFormatter secondary
    class whatsapp_web,anthropic,supabase,express,mcp external
    class fp_check,fp_identity,fp_response fastpath
```

## Explicação do Fluxo de Execução

### 1. **Inicialização do Sistema**
- `start.js` é o ponto de entrada que verifica o ambiente, dependências e configurações
- `start.js` carrega o arquivo `.env` e verifica variáveis obrigatórias
- Após as verificações, `start.js` importa e executa `src/index.js`

### 2. **Estrutura Principal**
- `src/index.js` define a classe `WhatsAppAISystem` que orquestra todos os componentes
- Inicializa os quatro componentes principais: WhatsAppBot, MultiAgentSystem, SupabaseExecutor e ResponseFormatter
- Configura um servidor Express para monitoramento e status

### 3. **⚡ Fast Path (NOVO)**
- **META_PATTERNS**: Regex que detecta perguntas meta/conversacionais
- **SYSTEM_IDENTITY**: Objeto predefinido com identidade, capacidades e tabelas conhecidas
- **Perguntas detectadas**: "o que você pode fazer?", "oi", "ajuda", "status"
- **Benefício**: Resposta em <50ms sem consultar banco de dados

### 4. **Componentes Principais**
- **WhatsAppBot** (`src/whatsapp/bot.js`): Gerencia a conexão com o WhatsApp usando whatsapp-web.js
- **MultiAgentSystem** (`src/ai/multiagent-system.js`): Implementa o sistema de IA com 5 agentes especializados:
  - 🎯 Coordinator Agent - Analisa intenção
  - 📋 Schema Agent - Descobre estrutura
  - 🔍 Query Agent - Constrói e executa SQL
  - 📊 Analyst Agent - Gera insights
  - 💬 Formatter Agent - Formata para WhatsApp
- **SupabaseExecutor** (`src/supabase/executor.js`): Gerencia a conexão e consultas ao Supabase
- **ResponseFormatter** (`src/formatters/response.js`): Formata as respostas para envio ao WhatsApp

### 5. **Fluxo de Processamento de Mensagens**
1. Mensagem recebida pelo WhatsAppBot
2. **Fast Path Check**: Verifica se é pergunta meta
   - **SIM**: Responde instantaneamente via SYSTEM_IDENTITY
   - **NÃO**: Segue para Full Path (5 agentes)
3. MultiAgentSystem analisa a intenção e consulta o SupabaseExecutor
4. Resposta é formatada pelo ResponseFormatter
5. WhatsAppBot envia a resposta formatada de volta ao usuário

### 6. **Dependências Externas**
- **whatsapp-web.js**: Comunicação com WhatsApp
- **Anthropic API (Claude 4)**: Processamento de linguagem natural
- **Supabase**: Armazenamento e consulta de dados PostgreSQL
- **MCP Server**: Execução de SQL via Model Context Protocol
- **Express**: Servidor web de monitoramento
