# 🏗️ Diagrama Mermaid - AI Data Scientist AICODEPRO

## 📊 Arquitetura Geral do Sistema com Fast Path

```mermaid
graph TB
    %% === CAMADA DE INTERFACE ===
    subgraph "📱 INTERFACE LAYER"
        WA[WhatsApp User]
        QR[QR Code Scanner]
    end

    %% === CAMADA DE APLICAÇÃO ===
    subgraph "🤖 APPLICATION LAYER"
        subgraph "📂 /src"
            INDEX[index.js<br/>🚀 System Orchestrator]
            
            subgraph "📂 /whatsapp"
                BOT[bot.js<br/>📱 WhatsApp Bot<br/>- Message Handler<br/>- QR Generation<br/>- Session Management]
            end
            
            subgraph "📂 /ai"
                MAS[multiagent-system.js<br/>🧠 Multi-Agent System<br/>- Coordinator Agent<br/>- Schema Agent<br/>- Query Agent<br/>- Analyst Agent<br/>- Formatter Agent]
                AGENT[agent.js<br/>🤖 Legacy AI Agent<br/>(MCP-based)]
            end
            
            subgraph "📂 /supabase"
                EXEC[executor.js<br/>🗄️ Supabase Executor<br/>- Dynamic Discovery<br/>- Query Execution<br/>- Pagination Handler<br/>- Cache Manager]
            end
            
            subgraph "📂 /formatters"
                RESP[response.js<br/>💬 Response Formatter<br/>- WhatsApp Formatting<br/>- Emoji Addition<br/>- Text Structuring]
            end
            
            subgraph "📂 /utils"
                HELP[helpers.js<br/>🛠️ Helper Functions<br/>- Config Validation<br/>- Number Formatting<br/>- Input Sanitization]
            end
        end
        
        subgraph "📂 /config"
            CONFIG[config.js<br/>⚙️ System Configuration<br/>- API Keys<br/>- Database URLs<br/>- Model Settings]
        end
    end

    %% === CAMADA DE DADOS ===
    subgraph "🗄️ DATA LAYER"
        SUPA[Supabase Database<br/>📊 PostgreSQL<br/>- Dynamic Tables<br/>- RPC Functions<br/>- Row Level Security]
    end

    %% === CAMADA DE IA ===
    subgraph "🧠 AI LAYER"
        CLAUDE[Claude 4 API<br/>🤖 Anthropic<br/>- Natural Language<br/>- SQL Generation<br/>- Data Analysis<br/>- Response Formatting]
    end

    %% === CAMADA DE CONFIGURAÇÃO ===
    subgraph "⚙️ CONFIG LAYER"
        ENV[.env<br/>🔐 Environment Variables]
        PKG[package.json<br/>📦 Dependencies]
        START[start.js<br/>🚀 Entry Point]
    end

    %% === CONEXÕES PRINCIPAIS ===
    WA --> BOT
    QR --> BOT
    BOT --> INDEX
    INDEX --> MAS
    INDEX --> EXEC
    INDEX --> RESP
    INDEX --> CONFIG
    MAS --> CLAUDE
    MAS --> EXEC
    EXEC --> SUPA
    CONFIG --> ENV
    START --> INDEX
    HELP --> CONFIG

    %% === ESTILOS ===
    classDef interface fill:#e1f5fe
    classDef application fill:#f3e5f5
    classDef data fill:#e8f5e8
    classDef ai fill:#fff3e0
    classDef config fill:#fce4ec

    class WA,QR interface
    class INDEX,BOT,MAS,AGENT,EXEC,RESP,HELP,CONFIG application
    class SUPA data
    class CLAUDE ai
    class ENV,PKG,START config
```

## ⚡ Fast Path - Respostas Instantâneas

```mermaid
flowchart LR
    subgraph "⚡ FAST PATH"
        MSG[📱 Mensagem] --> CHECK{META_PATTERNS?}
        CHECK -->|"'o que você pode fazer?'\n'oi', 'ajuda', 'status'"| IDENTITY[SYSTEM_IDENTITY]
        IDENTITY --> INSTANT[⚡ Resposta < 50ms]
        CHECK -->|"Consulta de dados"| FULL[Full Path\n5 Agentes]
    end
    
    subgraph "🎭 SYSTEM_IDENTITY"
        NAME[Nome: AI Data Scientist]
        CAPS[Capacidades: Ciência de Dados, BI]
        TABLES[Tabelas: 8 conhecidas]
    end
    
    IDENTITY --> NAME
    IDENTITY --> CAPS
    IDENTITY --> TABLES
```

## 🔄 Fluxo de Processamento Multi-Agentes

```mermaid
sequenceDiagram
    participant U as 👤 User
    participant WB as 📱 WhatsApp Bot
    participant IDX as 🚀 index.js
    participant MAS as 🧠 MultiAgent System
    participant FP as ⚡ Fast Path
    participant COORD as 🎯 Coordinator Agent
    participant SCHEMA as 📋 Schema Agent
    participant QUERY as 🔍 Query Agent
    participant ANALYST as 📊 Analyst Agent
    participant FORMAT as 💬 Formatter Agent
    participant EXEC as 🗄️ Supabase Executor
    participant SUPA as 🗄️ Supabase DB
    participant CLAUDE as 🤖 Claude 4

    %% === FASE 1: RECEPÇÃO ===
    U->>WB: "quantos leads temos?"
    WB->>IDX: handleMessage()
    IDX->>MAS: processMessage()

    %% === FASE 2: COORDENAÇÃO ===
    MAS->>COORD: coordinatorAgent()
    COORD->>EXEC: discoverAvailableTables()
    EXEC->>SUPA: list_tables RPC
    SUPA-->>EXEC: [tables with metadata]
    EXEC-->>COORD: available tables
    COORD->>CLAUDE: analyze intention + tables
    CLAUDE-->>COORD: {analysis_type: "count", tables_needed: ["leads"]}
    COORD-->>MAS: intention analysis

    %% === FASE 3: DESCOBERTA DE SCHEMA ===
    MAS->>SCHEMA: schemaAgent(["leads"])
    SCHEMA->>EXEC: describeTable("leads")
    EXEC->>SUPA: list_columns RPC
    SUPA-->>EXEC: columns metadata
    EXEC->>SUPA: sample data query
    SUPA-->>EXEC: sample records
    EXEC-->>SCHEMA: table structure
    SCHEMA-->>MAS: schema with columns

    %% === FASE 4: CONSTRUÇÃO DE QUERY ===
    MAS->>QUERY: queryAgent(intention, schema)
    QUERY->>CLAUDE: generate SQL query
    CLAUDE-->>QUERY: "SELECT COUNT(*) FROM leads"
    QUERY->>EXEC: executeSQLQuery()
    EXEC->>SUPA: execute query
    SUPA-->>EXEC: query results
    EXEC-->>QUERY: formatted results
    QUERY-->>MAS: query execution result

    %% === FASE 5: ANÁLISE ===
    MAS->>ANALYST: analystAgent(results, intention)
    ANALYST->>CLAUDE: analyze data + generate insights
    CLAUDE-->>ANALYST: business insights
    ANALYST-->>MAS: analysis with insights

    %% === FASE 6: FORMATAÇÃO ===
    MAS->>FORMAT: formatterAgent(analysis, results)
    FORMAT->>CLAUDE: format for WhatsApp
    CLAUDE-->>FORMAT: formatted response
    FORMAT-->>MAS: WhatsApp-ready text

    %% === FASE 7: RESPOSTA ===
    MAS-->>IDX: complete response
    IDX-->>WB: formatted message
    WB-->>U: "📊 Total: *1,247* leads"
```

## 🗂️ Estrutura de Arquivos e Responsabilidades

```mermaid
graph LR
    subgraph "📁 ROOT"
        A[start.js] --> B[src/index.js]
        C[package.json] --> B
        D[.env] --> E[config/config.js]
        E --> B
    end

    subgraph "📁 SRC"
        B --> F[whatsapp/bot.js]
        B --> G[ai/multiagent-system.js]
        B --> H[supabase/executor.js]
        B --> I[formatters/response.js]
        
        G --> J[ai/agent.js]
        
        K[utils/helpers.js] --> B
    end

    subgraph "📁 EXAMPLES"
        L[test-queries.js]
    end

    subgraph "📁 SCRIPTS"
        M[automation scripts]
    end

    %% Responsabilidades
    A -.-> |"🚀 Entry Point"| A
    B -.-> |"🎭 System Orchestrator"| B
    F -.-> |"📱 WhatsApp Interface"| F
    G -.-> |"🧠 AI Processing"| G
    H -.-> |"🗄️ Database Operations"| H
    I -.-> |"💬 Message Formatting"| I
    J -.-> |"🤖 Legacy AI (MCP)"| J
    K -.-> |"🛠️ Utilities"| K
```

## 🤖 Sistema Multi-Agentes Detalhado

```mermaid
graph TB
    subgraph "🧠 MULTI-AGENT SYSTEM"
        INPUT[📥 User Message<br/>"quantos leads únicos?"]
        
        subgraph "🎯 COORDINATOR AGENT"
            COORD_ANALYZE[Analyze Intention]
            COORD_DISCOVER[Discover Tables]
            COORD_PLAN[Plan Execution]
        end
        
        subgraph "📋 SCHEMA AGENT"
            SCHEMA_CACHE[Check Cache]
            SCHEMA_FETCH[Fetch Structure]
            SCHEMA_SAMPLE[Get Sample Data]
        end
        
        subgraph "🔍 QUERY AGENT"
            QUERY_BUILD[Build SQL]
            QUERY_OPTIMIZE[Optimize Query]
            QUERY_EXECUTE[Execute Query]
        end
        
        subgraph "📊 ANALYST AGENT"
            ANALYST_PROCESS[Process Results]
            ANALYST_INSIGHTS[Generate Insights]
            ANALYST_METRICS[Calculate Metrics]
        end
        
        subgraph "💬 FORMATTER AGENT"
            FORMAT_STRUCTURE[Structure Response]
            FORMAT_EMOJI[Add Emojis]
            FORMAT_WHATSAPP[WhatsApp Optimize]
        end
        
        OUTPUT[📤 Formatted Response<br/>"📊 *1,247* leads únicos"]
    end

    %% Fluxo sequencial
    INPUT --> COORD_ANALYZE
    COORD_ANALYZE --> COORD_DISCOVER
    COORD_DISCOVER --> COORD_PLAN
    COORD_PLAN --> SCHEMA_CACHE
    SCHEMA_CACHE --> SCHEMA_FETCH
    SCHEMA_FETCH --> SCHEMA_SAMPLE
    SCHEMA_SAMPLE --> QUERY_BUILD
    QUERY_BUILD --> QUERY_OPTIMIZE
    QUERY_OPTIMIZE --> QUERY_EXECUTE
    QUERY_EXECUTE --> ANALYST_PROCESS
    ANALYST_PROCESS --> ANALYST_INSIGHTS
    ANALYST_INSIGHTS --> ANALYST_METRICS
    ANALYST_METRICS --> FORMAT_STRUCTURE
    FORMAT_STRUCTURE --> FORMAT_EMOJI
    FORMAT_EMOJI --> FORMAT_WHATSAPP
    FORMAT_WHATSAPP --> OUTPUT

    %% Conexões com sistemas externos
    COORD_DISCOVER -.-> SUPABASE[(🗄️ Supabase)]
    SCHEMA_FETCH -.-> SUPABASE
    QUERY_EXECUTE -.-> SUPABASE
    
    COORD_ANALYZE -.-> CLAUDE[🤖 Claude 4]
    QUERY_BUILD -.-> CLAUDE
    ANALYST_INSIGHTS -.-> CLAUDE
    FORMAT_STRUCTURE -.-> CLAUDE
```

## 🗄️ Operações Supabase Executor

```mermaid
graph TB
    subgraph "🗄️ SUPABASE EXECUTOR"
        subgraph "🔍 DISCOVERY METHODS"
            DISC1[getTablesInfo<br/>📋 RPC list_tables]
            DISC2[enrichTablesWithColumns<br/>📊 RPC list_columns]
            DISC3[describeTable<br/>🔍 Table metadata]
        end
        
        subgraph "📊 DATA OPERATIONS"
            OP1[countRecords<br/>🔢 Simple counting]
            OP2[listRecords<br/>📝 Data listing]
            OP3[performAggregation<br/>📈 COUNT DISTINCT]
        end
        
        subgraph "⚡ OPTIMIZATION"
            OPT1[getAllRecordsWithPagination<br/>📄 Smart pagination]
            OPT2[Schema Cache<br/>💾 5min TTL]
            OPT3[Fallback Methods<br/>🔄 Error handling]
        end
        
        subgraph "🗄️ DATABASE"
            DB[(Supabase PostgreSQL<br/>- Dynamic Tables<br/>- RPC Functions<br/>- Row Level Security)]
        end
    end

    %% Conexões
    DISC1 --> DB
    DISC2 --> DB
    DISC3 --> DB
    OP1 --> DB
    OP2 --> DB
    OP3 --> OPT1
    OPT1 --> DB
    
    %% Cache connections
    DISC2 -.-> OPT2
    DISC3 -.-> OPT2
    
    %% Fallback connections
    DISC1 -.-> OPT3
    OP3 -.-> OPT3
```

## 📱 WhatsApp Bot Flow

```mermaid
stateDiagram-v2
    [*] --> Initializing
    Initializing --> QRGeneration: First time
    Initializing --> Authenticated: Session exists
    
    QRGeneration --> Authenticated: QR scanned
    
    Authenticated --> Listening: Ready
    
    Listening --> MessageReceived: User sends message
    MessageReceived --> Processing: handleMessage()
    Processing --> AIProcessing: processMessageWithAI()
    AIProcessing --> ResponseFormatting: AI response ready
    ResponseFormatting --> SendingResponse: Format for WhatsApp
    SendingResponse --> Listening: Message sent
    
    Processing --> Error: Invalid message
    AIProcessing --> Error: AI error
    Error --> Listening: Error message sent
    
    Authenticated --> Disconnected: Connection lost
    Disconnected --> QRGeneration: Reconnect needed
```

## 🔧 Configuração e Inicialização

```mermaid
graph TB
    subgraph "🚀 SYSTEM INITIALIZATION"
        START[start.js] --> INDEX[index.js]
        
        subgraph "⚙️ CONFIGURATION"
            ENV[.env file] --> CONFIG[config.js]
            PKG[package.json] --> DEPS[Dependencies]
        end
        
        subgraph "🏗️ COMPONENT SETUP"
            SETUP1[ResponseFormatter]
            SETUP2[SupabaseExecutor]
            SETUP3[MultiAgentSystem]
            SETUP4[WhatsAppBot]
            SETUP5[Express Server]
        end
        
        subgraph "✅ VALIDATION"
            TEST1[Supabase Connection]
            TEST2[WhatsApp Auth]
            TEST3[Claude API]
            TEST4[Health Endpoints]
        end
    end

    INDEX --> CONFIG
    CONFIG --> SETUP1
    SETUP1 --> SETUP2
    SETUP2 --> SETUP3
    SETUP3 --> SETUP4
    SETUP4 --> SETUP5
    
    SETUP2 --> TEST1
    SETUP4 --> TEST2
    SETUP3 --> TEST3
    SETUP5 --> TEST4
    
    TEST1 --> READY[🎉 System Ready]
    TEST2 --> READY
    TEST3 --> READY
    TEST4 --> READY
```

## 📊 Tipos de Análises Suportadas

```mermaid
mindmap
  root((📊 Análises))
    🔢 Contagem
      Simples
      Com filtros
      Distintas (COUNT DISTINCT)
    📝 Listagem
      Últimos registros
      Com ordenação
      Com paginação
    📈 Agregações
      Soma (SUM)
      Média (AVG)
      Máximo/Mínimo
    🔍 Filtros
      Por data
      Por texto
      Por domínio
    📊 Complexas
      Joins entre tabelas
      Análises temporais
      Insights de negócio
```

---

## 🎯 Pontos Críticos da Arquitetura

### ⚡ Performance
- **Cache de Schema**: 5 minutos TTL para metadados
- **Paginação Inteligente**: Resolve limite de 1000 registros
- **Descoberta Dinâmica**: Zero hardcode, adaptável a qualquer schema

### 🔒 Segurança
- **Row Level Security**: Políticas no Supabase
- **Service Role**: Acesso controlado via chaves
- **Sanitização**: Validação de inputs

### 🧠 Inteligência
- **Sistema Multi-Agentes**: 5 agentes especializados
- **Claude 4**: Processamento de linguagem natural
- **Análise Contextual**: Insights de negócio automáticos

### 📱 UX
- **Interface Natural**: Conversação via WhatsApp
- **Respostas Formatadas**: Emojis e estrutura otimizada
- **Feedback Imediato**: Status de "digitando..."