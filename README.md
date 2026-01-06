# 🤖 AI Data Scientist - AICODEPRO
## Sistema de BI Conversacional via WhatsApp com Multiagentes

### 📋 Visão Geral

Sistema avançado de **Ciência de Dados e Business Intelligence** que permite consultar bancos de dados Supabase através de mensagens em linguagem natural via WhatsApp. Utiliza um sistema multiagentes baseado em Claude Opus 4.5 com **Fast Path** para respostas instantâneas e **identidade contextualizada** para o negócio.

**Plataforma:** AICODEPRO / AI PRO EXPERT  
**Foco:** Cursos e treinamentos de Inteligência Artificial, Automação e Programação

### 🏗️ Arquitetura do Sistema

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   WhatsApp      │───▶│  Sistema         │───▶│   Supabase      │
│   (Interface)   │    │  Multiagentes    │    │   (Dados)       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │   Claude 4.5 opus│
                    │   (Inteligência) │
                    └──────────────────┘
```

### 🤖 Sistema Multiagentes

O sistema utiliza 5 agentes especializados que trabalham em conjunto:

#### 1. **Agente Coordenador**
- Analisa a intenção do usuário
- Determina que tipo de análise é necessária
- Planeja a execução da consulta

#### 2. **Agente Schema**
- Descobre dinamicamente a estrutura das tabelas
- Obtém metadados e amostras de dados
- Mantém cache para otimização

#### 3. **Agente Query**
- Constrói consultas SQL inteligentes
- Executa operações no Supabase
- Adapta estratégias conforme complexidade

#### 4. **Agente Analyst**
- Analisa os resultados obtidos
- Gera insights de negócio
- Identifica padrões e tendências

#### 5. **Agente Formatter**
- Formata respostas para WhatsApp
- Adiciona emojis e formatação
- Estrutura informações de forma clara

### ⚡ Fast Path - Respostas Instantâneas

O sistema implementa um **Fast Path** que detecta perguntas meta/conversacionais e responde **instantaneamente** (<50ms) sem consultar o banco de dados:

```
Mensagem → Fast Path (meta?) ──YES──▶ Resposta Instantânea
                │
               NO
                ▼
         Full Path (5 agentes)
```

**Perguntas detectadas pelo Fast Path:**
- "O que você pode fazer?" / "Quais suas capacidades?"
- "Oi" / "Olá" / "Bom dia" (saudações)
- "Obrigado" / "Valeu" (agradecimentos)
- "Status" / "Teste" / "Ping"
- "Ajuda" / "Help"

### 🎭 SYSTEM_IDENTITY - Identidade Contextualizada

O agente possui uma identidade pré-definida que conhece o contexto do negócio:

```javascript
name: "AI Data Scientist - AICODEPRO"
role: "Cientista de Dados & Business Intelligence"
company: "AICODEPRO / AI PRO EXPERT"
```

**Capacidades declaradas:**
- 🧪 **Ciência de Dados**: JOINs, subqueries, CTEs, análises estatísticas, coorte
- 📊 **Business Intelligence**: KPIs, funil de conversão, ROI, comparativos
- 🎓 **Análise Educacional**: performance de alunos, taxa de conclusão, abandono
- 🔍 **Queries Avançadas**: GROUP BY, window functions, CASE WHEN

**Tabelas conhecidas:**
- `aula_views` - Visualizações de aulas
- `aula_navigations` - Navegação entre aulas
- `qualified_leads` - Leads qualificados
- `engaged_leads` - Leads engajados
- `unified_leads` - Base consolidada
- `script_downloads` - Downloads de materiais
- `social_actions` - Redes sociais
- `whatsapp_actions` - WhatsApp

### 🚀 Funcionalidades

- ✅ **Fast Path**: Respostas instantâneas para perguntas meta (<50ms)
- ✅ **Identidade Contextualizada**: Agente conhece o negócio e suas capacidades
- ✅ **Descoberta Automática**: Identifica tabelas e colunas dinamicamente
- ✅ **Linguagem Natural**: Processa perguntas em português
- ✅ **Análises Complexas**: Suporta agregações, filtros, JOINs e CTEs
- ✅ **Insights Inteligentes**: Gera análises de negócio automaticamente
- ✅ **Cache Inteligente**: Otimiza performance com cache de metadados (5min TTL)
- ✅ **MCP Server**: Servidor interno para execução de SQL
- ✅ **Respostas Formatadas**: Saída otimizada para WhatsApp

### 📊 Exemplos de Uso

```
👤 "quais tabelas temos?"
🤖 Lista todas as tabelas disponíveis com contadores

👤 "quantos leads temos?"
🤖 Conta registros na tabela de leads

👤 "qual foi o registro mais recente na tabela aula_views?"
🤖 Busca e analisa o último registro com insights detalhados

👤 "leads com email gmail"
🤖 Filtra e conta leads com domínio Gmail

👤 "média de visualizações por usuário"
🤖 Calcula agregações complexas com análise
```

### 🛠️ Tecnologias Utilizadas

- **Node.js** - Runtime JavaScript
- **Claude 4.5 Opus** - LLM para inteligência artificial
- **Supabase** - Banco de dados PostgreSQL
- **WhatsApp Web.js** - Interface WhatsApp
- **Express.js** - Servidor web (Health Check)
- **Anthropic SDK** - Integração com Claude

### 📦 Instalação

1. **Clone o repositório**
```bash
git clone <repository-url>
cd whatsapp-ia-supabase
```

2. **Instale as dependências**
```bash
npm install
```

3. **Configure as variáveis de ambiente**
```bash
cp .env.example .env
```

4. **Configure o arquivo .env**
```env
# Claude (Principal)
ANTHROPIC_API_KEY=sk-ant-api03-...
ANTHROPIC_MODEL=claude-sonnet-4-20250514
ANTHROPIC_MAX_TOKENS=1500

# Supabase
SUPABASE_URL=https://seu-projeto.supabase.co
SUPABASE_KEY=sua-service-role-key

# WhatsApp
WHATSAPP_SESSION_PATH=./whatsapp-session-unified
WHATSAPP_CLIENT_ID=unified-ai-assistant

# Servidor
PORT=8080
```

5. **Execute o sistema**
```bash
npm start
```

### 🗄️ Configuração do Supabase

#### 🔧 Entendendo MCP Server vs Funções RPC

O sistema usa **dois mecanismos diferentes** para interagir com o Supabase:

| Componente | Função | Obrigatório? |
|------------|--------|--------------|
| **🔧 MCP Server** | Executa queries SQL nos dados | ✅ **SIM** - é o motor principal |
| **📋 Funções RPC** | Descobre estrutura do banco (metadados) | ❌ **NÃO** - tem fallbacks automáticos |

**Fluxo de execução:**
```
1. Usuário pergunta: "quantos leads temos?"
                    ↓
2. Sistema descobre tabelas disponíveis
   └── Usa RPC list_tables() OU fallback (information_schema)  ← OPCIONAL
                    ↓
3. Query Agent gera SQL: "SELECT COUNT(*) FROM leads"
                    ↓
4. MCP Server executa o SQL  ← PRINCIPAL (obrigatório)
                    ↓
5. Retorna resultado: 1982
```

**Resumo:** O **MCP Server já está configurado** e funciona automaticamente. As funções RPC abaixo são **opcionais** mas melhoram a performance da descoberta de tabelas.

---

#### 📋 Funções RPC (Opcionais - Recomendadas)

Estas funções **melhoram a performance** da descoberta de metadados, mas o sistema funciona sem elas (usa fallbacks automáticos via `information_schema`).

#### 1. **Função list_tables**
```sql
CREATE OR REPLACE FUNCTION list_tables()
RETURNS TABLE(table_name text) AS $$
BEGIN
    RETURN QUERY
    SELECT t.table_name::text
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
    AND t.table_type = 'BASE TABLE'
    ORDER BY t.table_name;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### 2. **Função list_columns**
```sql
CREATE OR REPLACE FUNCTION list_columns(table_name text)
RETURNS TABLE(
    column_name text,
    data_type text,
    is_nullable text
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.column_name::text,
        c.data_type::text,
        c.is_nullable::text
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
    AND c.table_name = $1
    ORDER BY c.ordinal_position;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### 3. **Função get_table_stats**
```sql
CREATE OR REPLACE FUNCTION get_table_stats(table_name text)
RETURNS TABLE(
    table_name text,
    row_count bigint,
    size_bytes bigint
) AS $$
DECLARE
    query_text text;
    row_count_result bigint;
BEGIN
    -- Conta registros dinamicamente
    query_text := format('SELECT COUNT(*) FROM %I', $1);
    EXECUTE query_text INTO row_count_result;
    
    RETURN QUERY
    SELECT 
        $1::text,
        row_count_result,
        pg_total_relation_size(format('%I', $1)::regclass)::bigint;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### 4. **Função execute_custom_query**
```sql
CREATE OR REPLACE FUNCTION execute_custom_query(
    query_text text,
    query_params text[] DEFAULT '{}'
)
RETURNS json AS $$
DECLARE
    result json;
BEGIN
    -- Validação de segurança - apenas SELECT permitido
    IF query_text !~* '^select' THEN
        RAISE EXCEPTION 'Apenas queries SELECT são permitidas';
    END IF;
    
    -- Remove comentários e caracteres perigosos
    IF query_text ~* '(drop|delete|update|insert|create|alter|truncate)' THEN
        RAISE EXCEPTION 'Operações de modificação não são permitidas';
    END IF;
    
    -- Executa query e retorna resultado como JSON
    EXECUTE format('SELECT row_to_json(t) FROM (%s) t', query_text) INTO result;
    RETURN result;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Erro na execução da query: %', SQLERRM;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### 5. **Função refresh_unified_leads** (se aplicável)
```sql
CREATE OR REPLACE FUNCTION refresh_unified_leads()
RETURNS void AS $$
BEGIN
    -- Atualiza tabela unificada de leads
    -- Implementar lógica específica do seu negócio
    RAISE NOTICE 'Unified leads refreshed at %', NOW();
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### 🔍 **Verificação das Funções RPC**

Após criar as funções, teste se estão funcionando:

```sql
-- Teste 1: Verificar se list_tables funciona
SELECT * FROM list_tables();

-- Teste 2: Verificar se list_columns funciona (substitua 'sua_tabela' por uma tabela real)
SELECT * FROM list_columns('sua_tabela');

-- Teste 3: Verificar se get_table_stats funciona
SELECT * FROM get_table_stats('sua_tabela');

-- Teste 4: Verificar se execute_custom_query funciona
SELECT execute_custom_query('SELECT 1 as teste');
```

**Status esperado**: Todas as funções devem retornar resultados sem erro.

### 🔐 Configuração de Segurança

#### Row Level Security (RLS)

Para tabelas sensíveis, configure políticas RLS:

```sql
-- Exemplo para tabela unified_leads
ALTER TABLE unified_leads ENABLE ROW LEVEL SECURITY;

-- Política para service_role (usado pelo sistema)
CREATE POLICY "service_role_access" ON unified_leads
FOR ALL TO service_role
USING (true);

-- Política para usuários autenticados (se necessário)
CREATE POLICY "authenticated_read" ON unified_leads
FOR SELECT TO authenticated
USING (true);
```

### 📁 Estrutura do Projeto - Visão Geral

```
src/
├── 📂 ai/                 # 🧠 CÉREBRO - Inteligência Artificial
├── 📂 formatters/         # 💬 BOCA - Formata respostas bonitas
├── 📂 mcp/                # 🔧 MOTOR - Executa SQL no banco
├── 📂 supabase/           # 🗄️ BRAÇO - Conecta e busca dados
├── 📂 utils/              # 🛠️ FERRAMENTAS - Funções auxiliares
├── 📂 whatsapp/           # 📱 OUVIDO - Recebe mensagens
└── 📄 index.js            # 🚀 MAESTRO - Orquestra tudo
```

---

#### 🚀 `index.js` - O MAESTRO
Ponto de entrada que inicializa e conecta todos os componentes:
```
Inicia → WhatsApp Bot → Sistema IA → Supabase → Servidor Express
```

---

#### 📱 `whatsapp/bot.js` - O OUVIDO
Conecta ao WhatsApp, gera QR Code, escuta mensagens e envia respostas.

---

#### 🧠 `ai/` - O CÉREBRO

| Arquivo | Função |
|---------|--------|
| `multiagent-system.js` | **Sistema principal** - 5 agentes + Fast Path + SYSTEM_IDENTITY |
| `agent.js` | Sistema legado (backup/referência) |

**Componentes do `multiagent-system.js`:**
- ⚡ **Fast Path** → Responde "oi", "ajuda" instantaneamente (<50ms)
- 🎯 **Coordinator Agent** → Entende a pergunta do usuário
- 📋 **Schema Agent** → Descobre estrutura das tabelas
- 🔍 **Query Agent** → Gera e executa SQL
- 📊 **Analyst Agent** → Analisa resultados e gera insights
- 💬 **Formatter Agent** → Formata para WhatsApp

---

#### 🔧 `mcp/supabase-server.js` - O MOTOR
Servidor interno que recebe SQL do Query Agent → Executa no banco → Retorna dados.
**Este é o componente principal de execução de queries.**

---

#### 🗄️ `supabase/executor.js` - O BRAÇO
Conecta ao Supabase, descobre tabelas/colunas dinamicamente, gerencia cache de schema.
Possui fallbacks automáticos se funções RPC não existirem.

---

#### 💬 `formatters/response.js` - A BOCA
Transforma dados brutos em respostas bonitas: adiciona emojis, formata números, estrutura para WhatsApp.

---

#### 🛠️ `utils/helpers.js` - FERRAMENTAS
Funções auxiliares: validações, formatação de números, sanitização de inputs.

---

### 🔄 Fluxo Visual Completo

```
📱 WhatsApp (ouvido)
      │
      ▼
🚀 index.js (maestro)
      │
      ▼
🧠 ai/multiagent-system.js (cérebro)
      │
      ├──⚡ Fast Path? ──YES──▶ Resposta instantânea (<50ms)
      │
      └──NO──▶ Full Path:
                  │
                  ├── 🗄️ supabase/executor.js (descobre tabelas)
                  │
                  ├── 🔧 mcp/supabase-server.js (executa SQL)
                  │
                  └── 💬 formatters/response.js (formata resposta)
                          │
                          ▼
                    📱 WhatsApp (envia resposta)
```

### 🔄 Fluxo de Processamento

```
┌─────────────────────────────────────────────────────────────┐
│                    MENSAGEM RECEBIDA                        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                 ⚡ FAST PATH CHECK                          │
│  Detecta: "o que você pode fazer?", "oi", "ajuda", etc     │
└─────────────────────────────────────────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
         [MATCH]                     [NO MATCH]
              │                           │
              ▼                           ▼
┌─────────────────────┐    ┌─────────────────────────────────┐
│ Resposta Instantânea│    │        FULL PATH                │
│ via SYSTEM_IDENTITY │    │  1. Coordinator → Intenção      │
│      (<50ms)        │    │  2. Schema → Estrutura          │
└─────────────────────┘    │  3. Query → SQL via MCP         │
                           │  4. Analyst → Insights          │
                           │  5. Formatter → WhatsApp        │
                           └─────────────────────────────────┘
                                          │
                                          ▼
                           ┌─────────────────────────────────┐
                           │      RESPOSTA ENVIADA           │
                           └─────────────────────────────────┘
```

### 📈 Exemplo de Análise Gerada

```
📊 *REGISTRO MAIS RECENTE - AULA VIEWS*

🕐 *Data/Hora:* 15 de maio de 2025, às 13:20
👤 *Usuário:* marco@rdantas.com.br
🎯 *Aula Acessada:* Aula 1

---

🔍 *DETALHES DO ACESSO:*
• *Navegação:* Aula 2 → Aula 1 (retorno/revisão)
• *Plataforma:* Windows Desktop + Chrome

💡 *INSIGHTS:*
• Padrão de revisão ativa - voltou da aula 2 para aula 1
• Indica engajamento e busca por reforço do conteúdo
```

### 🚀 Deploy e Produção

#### Variáveis de Ambiente de Produção
```env
NODE_ENV=production
PORT=8080
ANTHROPIC_API_KEY=sua-chave-producao
SUPABASE_URL=sua-url-producao
SUPABASE_KEY=sua-chave-producao
```

#### Monitoramento
- Logs estruturados para cada agente
- Métricas de performance de queries
- Cache hit/miss ratios
- Tempo de resposta por tipo de análise

### 🎓 Para Educadores

Este projeto demonstra:

- **Arquitetura Multiagentes**: Como dividir responsabilidades
- **Integração de APIs**: WhatsApp, Claude, Supabase
- **Processamento de Linguagem Natural**: Análise de intenções
- **Descoberta Dinâmica**: Sistema que se adapta aos dados
- **Análise de Dados**: Geração automática de insights
- **UX Conversacional**: Interface natural via chat

### 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

### 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo LICENSE para detalhes.

### 🔧 Troubleshooting

#### Problemas Comuns com Funções RPC

**❌ Erro: "Could not find the function public.list_tables()"**
- **Causa**: Função RPC não foi criada no Supabase
- **Solução**: Execute as funções SQL na seção "Configuração do Supabase"

**❌ Erro: "permission denied for function list_tables"**
- **Causa**: Função criada sem `SECURITY DEFINER`
- **Solução**: Recrie a função com `SECURITY DEFINER` no final

**❌ Erro: "RPC list_tables não disponível"**
- **Causa**: Service role key incorreta ou função não existe
- **Solução**:
  1. Verifique se `SUPABASE_KEY` é a service_role key (não anon key)
  2. Teste as funções no SQL Editor do Supabase

**❌ Sistema usa fallback em vez de RPC**
- **Causa**: Funções RPC não estão funcionando
- **Solução**: Execute os testes de verificação da seção anterior

### 🆘 Suporte

Para dúvidas ou problemas:
- Abra uma issue no GitHub
- Consulte a documentação do Supabase
- Verifique os logs do sistema
- Execute os testes de verificação das funções RPC

---

**Desenvolvido com ❤️ para demonstrar o poder da IA aplicada à análise de dados**