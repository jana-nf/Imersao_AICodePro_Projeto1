# Fluxo Detalhado de Processamento de Mensagem

Este diagrama ilustra o caminho detalhado que uma pergunta percorre no sistema **AI Data Scientist - AICODEPRO**, destacando o **Fast Path** para respostas instantâneas e o fluxo completo dos 5 agentes.

## 🔄 Fluxo de Processamento com Fast Path

```mermaid
flowchart TD
    %% Entrada da mensagem
    start([📱 Mensagem do Usuário]) --> whatsapp[WhatsApp Bot\nbot.js]
    
    %% Processamento inicial
    whatsapp --> |"1. Recebe mensagem"| ai_system[Sistema Multi-agente\nmultiagent-system.js]
    
    %% ⚡ FAST PATH - DESTAQUE TÉCNICO 1
    ai_system --> fast_path{⚡ FAST PATH\nMETA_PATTERNS}
    
    %% Fast Path - Respostas instantâneas
    fast_path --> |"MATCH: Pergunta meta\n'o que você pode fazer?'\n'oi', 'ajuda', 'status'"| identity[SYSTEM_IDENTITY\n- Nome: AI Data Scientist\n- Capacidades predefinidas\n- Tabelas conhecidas]
    identity --> instant_response[Resposta Instantânea\n< 50ms\nSem consulta ao banco]
    
    %% Full Path - Processamento completo
    fast_path --> |"NO MATCH:\nConsulta de dados"| coordinator[🎯 COORDINATOR AGENT\n- Analisa intenção\n- Descobre tabelas\n- Planeja execução]
    
    %% Cache de Schema
    coordinator --> schema_cache{Cache de\nSchema?}
    schema_cache --> |"HIT"| schema_cached[Schema em Cache\n5min TTL]
    schema_cache --> |"MISS"| schema_agent[📋 SCHEMA AGENT\n- list_columns RPC\n- Amostra de dados\n- Metadados]
    
    schema_cached --> query_agent
    schema_agent --> query_agent[🔍 QUERY AGENT\n- Gera SQL inteligente\n- JOINs, CTEs, Window Functions\n- Executa via MCP]
    
    %% Execução no Supabase
    query_agent --> mcp[MCP Server\nsupabase-server.js]
    mcp --> db[(Supabase DB\nPostgreSQL)]
    db --> results[Resultados Brutos]
    
    %% Análise de dados
    results --> analyst[📊 ANALYST AGENT\n- Gera insights\n- Detecta padrões\n- Métricas de negócio]
    
    %% Formatação para WhatsApp
    analyst --> formatter[💬 FORMATTER AGENT\n- Emojis contextuais\n- Markdown WhatsApp\n- Otimização mobile]
    
    %% Resposta final
    formatter --> final_response[Resposta Final]
    instant_response --> final_response
    final_response --> whatsapp_send[📤 Envio via WhatsApp]
    whatsapp_send --> end([✅ Mensagem Entregue])
    
    %% Estilo
    classDef fastpath fill:#90EE90,stroke:#333,stroke-width:2px;
    classDef agents fill:#87CEEB,stroke:#333,stroke-width:1px;
    classDef database fill:#DDA0DD,stroke:#333,stroke-width:1px;
    classDef identity fill:#FFD700,stroke:#333,stroke-width:2px;
    
    class fast_path,instant_response fastpath;
    class coordinator,schema_agent,query_agent,analyst,formatter agents;
    class db database;
    class identity identity;
```

## 💡 Destaques Técnicos Avançados

### 1. ⚡ Fast Path - Respostas Instantâneas (NOVO)
- **Implementação**: Detecção de perguntas meta via `META_PATTERNS` (regex)
- **Benefício**: Resposta em <50ms sem consultar banco de dados
- **Código**: `handleFastPath()` verifica padrões antes de acionar agentes
- **Diferencial**: Perguntas como "o que você pode fazer?", "oi", "ajuda" respondem instantaneamente
- **Padrões detectados**:
  - Capacidades: "o que você pode fazer?", "quais suas capacidades?"
  - Saudações: "oi", "olá", "bom dia"
  - Agradecimentos: "obrigado", "valeu"
  - Status: "teste", "ping", "status"

### 2. 🎭 SYSTEM_IDENTITY - Identidade Contextualizada (NOVO)
- **Implementação**: Objeto predefinido com identidade, capacidades e tabelas conhecidas
- **Benefício**: Agente sabe quem é e o que pode fazer sem consultar banco
- **Código**: `SYSTEM_IDENTITY` no construtor do `MultiAgentSystem`
- **Diferencial**: Respostas consistentes e contextualizadas para o negócio AICODEPRO
- **Conteúdo**:
  - Nome: "AI Data Scientist - AICODEPRO"
  - Role: "Cientista de Dados & Business Intelligence"
  - Capacidades: Ciência de Dados, BI, Análise Educacional, Queries Avançadas
  - Tabelas conhecidas: 8 tabelas documentadas

### 3. Cache de Schema (5min TTL)
- **Implementação**: `schemaCache` Map com timestamp de expiração
- **Benefício**: Evita redescoberta de estrutura a cada mensagem
- **Código**: Verificação de TTL antes de chamar `schemaAgent()`
- **Diferencial**: Reduz latência de ~500ms para ~5ms em consultas repetidas

### 4. MCP Server Interno
- **Implementação**: Servidor MCP para execução de SQL
- **Benefício**: Execução segura e padronizada de queries
- **Código**: `supabase-server.js` com tool `execute_sql`
- **Diferencial**: Permite queries complexas (JOINs, CTEs, Window Functions)

### 5. Sistema de 5 Agentes Especializados
- **Coordinator**: Analisa intenção e planeja execução
- **Schema**: Descobre estrutura das tabelas
- **Query**: Constrói e executa SQL inteligente
- **Analyst**: Gera insights de negócio
- **Formatter**: Formata para WhatsApp
