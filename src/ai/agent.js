const Anthropic = require('@anthropic-ai/sdk');

class AIAgent {
    constructor(supabaseExecutor) {
        this.supabaseExecutor = supabaseExecutor;
        
        // Inicializa Claude 4 do .env
        this.anthropic = new Anthropic({
            apiKey: process.env.ANTHROPIC_API_KEY,
        });
        
        this.model = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514';
        this.maxTokens = parseInt(process.env.ANTHROPIC_MAX_TOKENS) || 1000;
        this.isReady = true;
        
        console.log(`🧠 Agente IA Claude 4 inicializado (${this.model})`);
        
        // Inicializa MCP tools
        this.mcpTools = this.initializeMCPTools();
    }

    initializeMCPTools() {
        return [
            {
                name: "list_databases",
                description: "Lista informações sobre o banco de dados Supabase",
                input_schema: {
                    type: "object",
                    properties: {}
                }
            },
            {
                name: "list_tables", 
                description: "Lista todas as tabelas disponíveis no banco",
                input_schema: {
                    type: "object",
                    properties: {
                        include_system: {
                            type: "boolean",
                            description: "Incluir tabelas do sistema"
                        }
                    }
                }
            },
            {
                name: "describe_table",
                description: "Descreve a estrutura de uma tabela específica",
                input_schema: {
                    type: "object",
                    properties: {
                        table_name: {
                            type: "string",
                            description: "Nome da tabela para descrever"
                        }
                    },
                    required: ["table_name"]
                }
            },
            {
                name: "count_records",
                description: "Conta registros em uma tabela com filtros opcionais",
                input_schema: {
                    type: "object",
                    properties: {
                        table_name: {
                            type: "string",
                            description: "Nome da tabela"
                        },
                        filters: {
                            type: "array",
                            description: "Filtros para aplicar",
                            items: {
                                type: "object",
                                properties: {
                                    column: { type: "string" },
                                    operator: { type: "string" },
                                    value: { type: "string" }
                                }
                            }
                        }
                    },
                    required: ["table_name"]
                }
            },
            {
                name: "query_records",
                description: "Consulta registros de uma tabela com filtros, ordenação e limite",
                input_schema: {
                    type: "object",
                    properties: {
                        table_name: {
                            type: "string",
                            description: "Nome da tabela"
                        },
                        columns: {
                            type: "array",
                            description: "Colunas para selecionar",
                            items: { type: "string" }
                        },
                        filters: {
                            type: "array",
                            description: "Filtros para aplicar"
                        },
                        order_by: {
                            type: "object",
                            properties: {
                                column: { type: "string" },
                                ascending: { type: "boolean" }
                            }
                        },
                        limit: {
                            type: "integer",
                            description: "Número máximo de registros"
                        }
                    },
                    required: ["table_name"]
                }
            },
            {
                name: "aggregate_data",
                description: "Executa agregações como COUNT DISTINCT, SUM, AVG, etc.",
                input_schema: {
                    type: "object",
                    properties: {
                        table_name: {
                            type: "string",
                            description: "Nome da tabela"
                        },
                        operation: {
                            type: "string",
                            enum: ["count_distinct", "sum", "avg", "max", "min"],
                            description: "Tipo de agregação"
                        },
                        column: {
                            type: "string",
                            description: "Coluna para agregar"
                        },
                        filters: {
                            type: "array",
                            description: "Filtros para aplicar antes da agregação"
                        }
                    },
                    required: ["table_name", "operation", "column"]
                }
            }
        ];
    }

    async processMessage(messageText, userContext) {
        try {
            console.log(`🧠 Claude 4 processando: "${messageText}"`);

            // Usa Claude 4 com MCP tools para raciocinar e agir
            const response = await this.anthropic.messages.create({
                model: this.model,
                max_tokens: this.maxTokens,
                tools: this.mcpTools,
                messages: [
                    {
                        role: 'user',
                        content: messageText
                    }
                ],
                system: this.buildSystemPrompt()
            });

            console.log(`🧠 Claude 4 respondeu:`, JSON.stringify(response, null, 2));

            // Processa a resposta do Claude
            return await this.processClaudeResponse(response, messageText, userContext);

        } catch (error) {
            console.error('❌ Erro no agente IA:', error);
            return {
                analysis: { error: true },
                result: { success: false, error: error.message },
                response: `🤖 Desculpe, encontrei um erro: ${error.message}`,
                userContext: userContext
            };
        }
    }

    buildSystemPrompt() {
        return `Você é um agente IA especialista em análise de dados SQL, trabalhando com um banco Supabase via MCP (Model Context Protocol).

SUAS CAPACIDADES:
1. Analisar solicitações em linguagem natural sobre dados
2. Usar tools MCP para interagir com o banco Supabase
3. Raciocinar sobre os dados e fornecer insights
4. Responder de forma clara e amigável para WhatsApp

TOOLS MCP DISPONÍVEIS:
- list_databases: Informações sobre o banco
- list_tables: Lista tabelas disponíveis  
- describe_table: Estrutura de uma tabela
- count_records: Conta registros com filtros
- query_records: Consulta dados com filtros/ordenação
- aggregate_data: Agregações (COUNT DISTINCT, SUM, etc.)

EXEMPLOS DE USO:
- "qual database temos?" → use list_databases
- "quais tabelas?" → use list_tables
- "quantos clientes?" → use count_records com table_name="clientes"
- "últimos 5 pedidos" → use query_records com limit=5 e order_by
- "linhas distintas na coluna email" → use aggregate_data com operation="count_distinct"

INSTRUÇÕES:
1. SEMPRE use as tools MCP para obter dados reais
2. Raciocine sobre os resultados antes de responder
3. Formate respostas para WhatsApp (emojis, negrito com *)
4. Se não souber qual tabela usar, liste as tabelas primeiro
5. Para perguntas complexas, use múltiplas tools em sequência

FORMATO DE RESPOSTA:
- Use emojis apropriados (📊 📋 🔢 ✅ ❌)
- Destaque números importantes com *negrito*
- Seja conciso mas informativo
- Máximo 4000 caracteres`;
    }

    async processClaudeResponse(response, originalMessage, userContext) {
        try {
            let finalResponse = '';
            let toolResults = [];

            // Processa cada parte da resposta
            for (const content of response.content) {
                if (content.type === 'text') {
                    finalResponse += content.text + '\n';
                } else if (content.type === 'tool_use') {
                    // Executa a tool via MCP
                    const toolResult = await this.executeMCPTool(content.name, content.input);
                    toolResults.push({
                        tool: content.name,
                        input: content.input,
                        result: toolResult
                    });

                    // Se Claude não forneceu texto, gera resposta baseada no resultado
                    if (!finalResponse.trim()) {
                        finalResponse = await this.generateResponseFromToolResult(content.name, toolResult);
                    }
                }
            }

            // Se Claude usou tools, SEMPRE formata uma resposta
            if (toolResults.length > 0) {
                console.log('🔧 Processando resultados das tools...');
                
                // Primeiro tenta usar o resultado direto da primeira tool
                if (toolResults.length > 0) {
                    finalResponse = await this.generateResponseFromToolResult(
                        toolResults[0].tool,
                        toolResults[0].result
                    );
                    console.log('📝 Resposta gerada:', finalResponse.substring(0, 100) + '...');
                }
                
                // Se ainda não tem resposta, tenta formatação com Claude
                if (!finalResponse.trim()) {
                    finalResponse = await this.formatToolResults(toolResults, originalMessage);
                }
            }

            // Garante que SEMPRE há uma resposta não-vazia
            const finalResponseText = finalResponse.trim() || '🤖 Processamento concluído, mas não consegui gerar uma resposta adequada.';
            
            console.log('✅ Resposta final preparada:', finalResponseText.length, 'caracteres');

            return {
                analysis: {
                    message: originalMessage,
                    tools_used: toolResults.map(t => t.tool),
                    reasoning: "Claude 4 analisou e executou tools MCP"
                },
                result: {
                    success: true,
                    tool_results: toolResults
                },
                response: finalResponseText,
                userContext: userContext
            };

        } catch (error) {
            console.error('❌ Erro ao processar resposta Claude:', error);
            throw error;
        }
    }

    async executeMCPTool(toolName, toolInput) {
        try {
            console.log(`🔧 Executando MCP tool: ${toolName}`, toolInput);

            switch (toolName) {
                case 'list_databases':
                    return {
                        database: 'Supabase PostgreSQL',
                        url: process.env.SUPABASE_URL,
                        schema: 'public',
                        version: 'PostgreSQL 15+',
                        features: ['REST API', 'Real-time', 'Auth', 'Storage']
                    };

                case 'list_tables':
                    return await this.supabaseExecutor.listTables();

                case 'describe_table':
                    return await this.supabaseExecutor.describeTable(toolInput.table_name);

                case 'count_records':
                    return await this.supabaseExecutor.countRecords(
                        toolInput.table_name,
                        toolInput.filters || []
                    );

                case 'query_records':
                    return await this.supabaseExecutor.listRecords(toolInput.table_name, {
                        filters: toolInput.filters || [],
                        limit: toolInput.limit || 10,
                        orderBy: toolInput.order_by,
                        columns: toolInput.columns
                    });

                case 'aggregate_data':
                    return await this.supabaseExecutor.performAggregation(
                        toolInput.table_name,
                        {
                            type: toolInput.operation,
                            column: toolInput.column,
                            filters: toolInput.filters || []
                        }
                    );

                default:
                    throw new Error(`Tool MCP não implementada: ${toolName}`);
            }

        } catch (error) {
            console.error(`❌ Erro ao executar tool ${toolName}:`, error);
            return {
                success: false,
                error: error.message
            };
        }
    }

    async generateResponseFromToolResult(toolName, result) {
        try {
            // Gera resposta formatada baseada no resultado da tool
            if (!result.success) {
                return `❌ *Erro*\n\n${result.error}`;
            }

            switch (toolName) {
                case 'list_databases':
                    return `🗄️ *Banco de Dados*\n\n*Database:* ${result.database}\n*Schema:* ${result.schema}\n*Versão:* ${result.version}`;

                case 'list_tables':
                    const tables = result.data || [];
                    if (tables.length === 0) {
                        return '📊 *Tabelas*\n\nNenhuma tabela encontrada.';
                    }
                    
                    let response = `📊 *Tabelas Disponíveis* (${tables.length})\n\n`;
                    tables.forEach((table, i) => {
                        response += `${i + 1}. *${table.table_name}*\n`;
                        if (table.row_count !== undefined) {
                            response += `   📊 ${table.row_count} registros\n`;
                        }
                        response += '\n';
                    });
                    return response;

                case 'count_records':
                    const count = result.data?.count || 0;
                    return `📊 *Contagem*\n\n🔢 Total: *${count.toLocaleString('pt-BR')}* registros`;

                case 'query_records':
                    const records = result.data || [];
                    if (records.length === 0) {
                        return '📋 *Consulta*\n\nNenhum registro encontrado.';
                    }
                    
                    let recordsResponse = `📋 *Registros* (${records.length})\n\n`;
                    records.slice(0, 5).forEach((record, i) => {
                        recordsResponse += `${i + 1}. ${this.formatRecord(record)}\n\n`;
                    });
                    
                    if (records.length > 5) {
                        recordsResponse += `⚠️ Mostrando apenas 5 de ${records.length} registros.`;
                    }
                    
                    return recordsResponse;

                default:
                    return `🤖 *Resultado*\n\n${JSON.stringify(result, null, 2)}`;
            }

        } catch (error) {
            return `❌ Erro ao formatar resposta: ${error.message}`;
        }
    }

    formatRecord(record) {
        const keys = Object.keys(record);
        const importantFields = keys.slice(0, 3); // Primeiros 3 campos
        
        let text = '';
        importantFields.forEach(key => {
            const value = record[key];
            if (value !== null && value !== undefined) {
                text += `${key}: ${value}, `;
            }
        });
        
        return text.slice(0, -2); // Remove última vírgula
    }

    async formatToolResults(toolResults, originalMessage) {
        try {
            // Usa Claude para formatar os resultados das tools
            const resultsText = toolResults.map(tr => 
                `Tool: ${tr.tool}\nInput: ${JSON.stringify(tr.input)}\nResult: ${JSON.stringify(tr.result)}`
            ).join('\n\n');

            const response = await this.anthropic.messages.create({
                model: this.model,
                max_tokens: 800,
                messages: [
                    {
                        role: 'user',
                        content: `Formate estes resultados de tools MCP para uma resposta amigável no WhatsApp sobre: "${originalMessage}"\n\nResultados:\n${resultsText}`
                    }
                ],
                system: 'Formate a resposta para WhatsApp usando emojis, negrito (*texto*) e seja conciso. Máximo 4000 caracteres.'
            });

            return response.content[0].text;

        } catch (error) {
            console.error('❌ Erro ao formatar com Claude:', error);
            return '🤖 Resultados processados com sucesso!';
        }
    }

    async testAI() {
        try {
            const response = await this.anthropic.messages.create({
                model: this.model,
                max_tokens: 50,
                messages: [
                    {
                        role: 'user',
                        content: 'Teste de conexão - responda apenas "OK"'
                    }
                ]
            });

            console.log('🧠 Claude 4 respondeu:', response.content[0].text);
            return true;
        } catch (error) {
            console.error('❌ Erro no teste Claude 4:', error);
            return false;
        }
    }
}

module.exports = AIAgent;