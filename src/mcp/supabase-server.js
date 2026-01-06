#!/usr/bin/env node

/**
 * Servidor MCP para Supabase
 * Implementa o Model Context Protocol para acesso inteligente ao banco
 */

const { Server } = require('@modelcontextprotocol/sdk/server/index.js');
const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
const { CallToolRequestSchema, ListToolsRequestSchema } = require('@modelcontextprotocol/sdk/types.js');
const { createClient } = require('@supabase/supabase-js');

class SupabaseMCPServer {
    constructor() {
        this.server = new Server(
            {
                name: 'supabase-mcp-server',
                version: '1.0.0',
            },
            {
                capabilities: {
                    tools: {},
                },
            }
        );

        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_KEY
        );

        this.setupToolHandlers();
        this.setupResourceHandlers();
    }

    setupToolHandlers() {
        // Lista todas as tools disponíveis
        this.server.setRequestHandler(ListToolsRequestSchema, async () => {
            return {
                tools: [
                    {
                        name: 'list_databases',
                        description: 'Lista informações sobre o banco de dados Supabase',
                        inputSchema: {
                            type: 'object',
                            properties: {},
                        },
                    },
                    {
                        name: 'list_tables',
                        description: 'Lista todas as tabelas disponíveis no banco',
                        inputSchema: {
                            type: 'object',
                            properties: {
                                include_system: {
                                    type: 'boolean',
                                    description: 'Incluir tabelas do sistema',
                                    default: false
                                }
                            },
                        },
                    },
                    {
                        name: 'describe_table',
                        description: 'Descreve a estrutura de uma tabela específica',
                        inputSchema: {
                            type: 'object',
                            properties: {
                                table_name: {
                                    type: 'string',
                                    description: 'Nome da tabela para descrever',
                                },
                            },
                            required: ['table_name'],
                        },
                    },
                    {
                        name: 'count_records',
                        description: 'Conta registros em uma tabela com filtros opcionais',
                        inputSchema: {
                            type: 'object',
                            properties: {
                                table_name: {
                                    type: 'string',
                                    description: 'Nome da tabela',
                                },
                                filters: {
                                    type: 'array',
                                    description: 'Filtros para aplicar',
                                    items: {
                                        type: 'object',
                                        properties: {
                                            column: { type: 'string' },
                                            operator: { type: 'string', enum: ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'like', 'ilike'] },
                                            value: { type: 'string' }
                                        }
                                    }
                                }
                            },
                            required: ['table_name'],
                        },
                    },
                    {
                        name: 'query_records',
                        description: 'Consulta registros de uma tabela com filtros, ordenação e limite',
                        inputSchema: {
                            type: 'object',
                            properties: {
                                table_name: {
                                    type: 'string',
                                    description: 'Nome da tabela',
                                },
                                columns: {
                                    type: 'array',
                                    description: 'Colunas para selecionar (vazio = todas)',
                                    items: { type: 'string' }
                                },
                                filters: {
                                    type: 'array',
                                    description: 'Filtros para aplicar',
                                    items: {
                                        type: 'object',
                                        properties: {
                                            column: { type: 'string' },
                                            operator: { type: 'string' },
                                            value: { type: 'string' }
                                        }
                                    }
                                },
                                order_by: {
                                    type: 'object',
                                    properties: {
                                        column: { type: 'string' },
                                        ascending: { type: 'boolean', default: true }
                                    }
                                },
                                limit: {
                                    type: 'integer',
                                    description: 'Número máximo de registros',
                                    default: 10
                                }
                            },
                            required: ['table_name'],
                        },
                    },
                    {
                        name: 'aggregate_data',
                        description: 'Executa agregações como COUNT DISTINCT, SUM, AVG, etc.',
                        inputSchema: {
                            type: 'object',
                            properties: {
                                table_name: {
                                    type: 'string',
                                    description: 'Nome da tabela',
                                },
                                operation: {
                                    type: 'string',
                                    enum: ['count_distinct', 'sum', 'avg', 'max', 'min'],
                                    description: 'Tipo de agregação'
                                },
                                column: {
                                    type: 'string',
                                    description: 'Coluna para agregar'
                                },
                                filters: {
                                    type: 'array',
                                    description: 'Filtros para aplicar antes da agregação',
                                    items: {
                                        type: 'object',
                                        properties: {
                                            column: { type: 'string' },
                                            operator: { type: 'string' },
                                            value: { type: 'string' }
                                        }
                                    }
                                }
                            },
                            required: ['table_name', 'operation', 'column'],
                        },
                    },
                    {
                        name: 'execute_sql',
                        description: 'Executa uma query SQL personalizada (somente SELECT)',
                        inputSchema: {
                            type: 'object',
                            properties: {
                                query: {
                                    type: 'string',
                                    description: 'Query SQL para executar (apenas SELECT)',
                                },
                                parameters: {
                                    type: 'array',
                                    description: 'Parâmetros para a query',
                                    items: { type: 'string' }
                                }
                            },
                            required: ['query'],
                        },
                    }
                ],
            };
        });

        // Executa as tools
        this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
            const { name, arguments: args } = request.params;

            try {
                switch (name) {
                    case 'list_databases':
                        return await this.listDatabases();
                    
                    case 'list_tables':
                        return await this.listTables(args.include_system || false);
                    
                    case 'describe_table':
                        return await this.describeTable(args.table_name);
                    
                    case 'count_records':
                        return await this.countRecords(args.table_name, args.filters || []);
                    
                    case 'query_records':
                        return await this.queryRecords(args);
                    
                    case 'aggregate_data':
                        return await this.aggregateData(args);
                    
                    case 'execute_sql':
                        return await this.executeSQL(args.query, args.parameters || []);
                    
                    default:
                        throw new Error(`Tool desconhecida: ${name}`);
                }
            } catch (error) {
                return {
                    content: [
                        {
                            type: 'text',
                            text: `Erro ao executar ${name}: ${error.message}`,
                        },
                    ],
                    isError: true,
                };
            }
        });
    }

    setupResourceHandlers() {
        // Implementar recursos MCP se necessário
    }

    async listDatabases() {
        return {
            content: [
                {
                    type: 'text',
                    text: JSON.stringify({
                        database: 'Supabase PostgreSQL',
                        url: process.env.SUPABASE_URL,
                        schema: 'public',
                        version: 'PostgreSQL 15+',
                        features: ['REST API', 'Real-time', 'Auth', 'Storage']
                    }, null, 2),
                },
            ],
        };
    }

    async listTables(includeSystem = false) {
        try {
            // Descoberta automática de tabelas
            const commonTables = [
                'users', 'usuarios', 'clientes', 'leads', 'produtos', 'pedidos', 'vendas', 'funcionarios',
                'empresas', 'contatos', 'campanhas', 'oportunidades', 'tarefas', 'projetos',
                'customers', 'products', 'orders', 'sales', 'employees', 'companies', 'contacts',
                'campaigns', 'opportunities', 'tasks', 'projects', 'invoices', 'payments',
                'profiles', 'settings', 'logs', 'notifications', 'categories', 'tags'
            ];

            const foundTables = [];

            for (const tableName of commonTables) {
                try {
                    const { data, error, count } = await this.supabase
                        .from(tableName)
                        .select('*', { count: 'exact', head: true });

                    if (!error) {
                        // Busca estrutura da tabela
                        const { data: sampleData } = await this.supabase
                            .from(tableName)
                            .select('*')
                            .limit(1);

                        const columns = sampleData && sampleData.length > 0 ? Object.keys(sampleData[0]) : [];
                        
                        foundTables.push({
                            table_name: tableName,
                            columns: columns,
                            row_count: count || 0,
                            column_count: columns.length
                        });
                    }
                } catch (err) {
                    // Ignora erros
                }
            }

            return {
                content: [
                    {
                        type: 'text',
                        text: JSON.stringify({
                            tables: foundTables,
                            total_tables: foundTables.length,
                            discovery_method: 'automatic_probing'
                        }, null, 2),
                    },
                ],
            };
        } catch (error) {
            throw new Error(`Erro ao listar tabelas: ${error.message}`);
        }
    }

    async describeTable(tableName) {
        try {
            // Busca estrutura e dados de exemplo
            const { data: sampleData } = await this.supabase
                .from(tableName)
                .select('*')
                .limit(1);

            const { count } = await this.supabase
                .from(tableName)
                .select('*', { count: 'exact', head: true });

            if (sampleData === null) {
                throw new Error(`Tabela '${tableName}' não encontrada`);
            }

            const columns = sampleData && sampleData.length > 0 ? Object.keys(sampleData[0]) : [];
            const sampleRecord = sampleData && sampleData.length > 0 ? sampleData[0] : null;

            return {
                content: [
                    {
                        type: 'text',
                        text: JSON.stringify({
                            table_name: tableName,
                            columns: columns,
                            column_count: columns.length,
                            row_count: count || 0,
                            sample_record: sampleRecord
                        }, null, 2),
                    },
                ],
            };
        } catch (error) {
            throw new Error(`Erro ao descrever tabela: ${error.message}`);
        }
    }

    async countRecords(tableName, filters = []) {
        try {
            let query = this.supabase
                .from(tableName)
                .select('*', { count: 'exact', head: true });

            // Aplica filtros
            query = this.applyFilters(query, filters);

            const { count, error } = await query;

            if (error) {
                throw new Error(error.message);
            }

            return {
                content: [
                    {
                        type: 'text',
                        text: JSON.stringify({
                            table_name: tableName,
                            count: count,
                            filters_applied: filters
                        }, null, 2),
                    },
                ],
            };
        } catch (error) {
            throw new Error(`Erro ao contar registros: ${error.message}`);
        }
    }

    async queryRecords(args) {
        try {
            const { table_name, columns, filters, order_by, limit } = args;

            let query = this.supabase.from(table_name);

            // Seleciona colunas - CORRIGIDO: valida colunas antes de usar
            if (columns && columns.length > 0) {
                // Remove colunas com funções SQL que o Supabase não suporta via REST
                const validColumns = columns.filter(col => {
                    const cleanCol = col.trim();
                    return !cleanCol.includes('(') &&
                           !cleanCol.includes('count') &&
                           !cleanCol.includes('case') &&
                           !cleanCol.includes('when') &&
                           cleanCol.length > 0;
                });
                
                if (validColumns.length > 0) {
                    query = query.select(validColumns.join(', '));
                } else {
                    // Se não há colunas válidas, usa SELECT *
                    query = query.select('*');
                }
            } else {
                query = query.select('*');
            }

            // Aplica filtros
            query = this.applyFilters(query, filters || []);

            // Aplica ordenação
            if (order_by) {
                query = query.order(order_by.column, { ascending: order_by.ascending });
            }

            // Aplica limite
            if (limit) {
                query = query.limit(limit);
            }

            const { data, error } = await query;

            if (error) {
                throw new Error(error.message);
            }

            return {
                content: [
                    {
                        type: 'text',
                        text: JSON.stringify({
                            table_name: table_name,
                            records: data,
                            count: data.length,
                            query_params: args
                        }, null, 2),
                    },
                ],
            };
        } catch (error) {
            throw new Error(`Erro ao consultar registros: ${error.message}`);
        }
    }

    async aggregateData(args) {
        try {
            const { table_name, operation, column, filters } = args;

            if (operation === 'count_distinct') {
                // Para COUNT DISTINCT, busca todos os valores únicos
                let query = this.supabase
                    .from(table_name)
                    .select(column);

                query = this.applyFilters(query, filters || []);

                const { data, error } = await query;

                if (error) {
                    throw new Error(error.message);
                }

                const uniqueValues = new Set(data.map(row => row[column]));

                return {
                    content: [
                        {
                            type: 'text',
                            text: JSON.stringify({
                                table_name: table_name,
                                operation: 'count_distinct',
                                column: column,
                                result: uniqueValues.size,
                                filters_applied: filters || []
                            }, null, 2),
                        },
                    ],
                };
            }

            // Para outras agregações, implementar conforme necessário
            throw new Error(`Operação de agregação '${operation}' não implementada ainda`);

        } catch (error) {
            throw new Error(`Erro na agregação: ${error.message}`);
        }
    }

    async executeSQL(query, parameters = []) {
        try {
            // Validação básica de segurança
            if (!query.toLowerCase().trim().startsWith('select')) {
                throw new Error('Apenas queries SELECT são permitidas');
            }

            // Tenta usar RPC execute_custom_query primeiro
            try {
                const { data, error } = await this.supabase.rpc('execute_custom_query', {
                    query_text: query,
                    query_params: parameters
                });

                if (error) {
                    throw new Error(error.message);
                }

                return {
                    content: [
                        {
                            type: 'text',
                            text: JSON.stringify({
                                query: query,
                                parameters: parameters,
                                result: Array.isArray(data) ? data : [data],
                                method: 'rpc_execute_custom_query'
                            }, null, 2),
                        },
                    ],
                };
            } catch (rpcError) {
                console.log('⚠️ RPC execute_custom_query falhou, tentando fallback...');
                
                // Fallback: Converte SQL simples para operações Supabase
                return await this.executeSimpleSQLFallback(query);
            }

        } catch (error) {
            throw new Error(`Erro ao executar SQL: ${error.message}`);
        }
    }

    async executeSimpleSQLFallback(query) {
        const sql = query.toLowerCase().trim();
        
        // Para COUNT simples
        if (sql.includes('count(*)') && sql.includes('from')) {
            const tableMatch = sql.match(/from\s+(\w+)/);
            if (tableMatch) {
                const tableName = tableMatch[1];
                
                // Detecta WHERE simples
                let filters = [];
                if (sql.includes('where')) {
                    const whereMatch = sql.match(/where\s+(\w+)\s*=\s*'([^']+)'/);
                    if (whereMatch) {
                        filters = [{
                            column: whereMatch[1],
                            operator: 'eq',
                            value: whereMatch[2]
                        }];
                    }
                }
                
                return await this.countRecords(tableName, filters);
            }
        }
        
        // Para SELECT simples
        if (sql.includes('select') && sql.includes('from')) {
            const tableMatch = sql.match(/from\s+(\w+)/);
            if (tableMatch) {
                const tableName = tableMatch[1];
                
                const args = { table_name: tableName };
                
                // Detecta WHERE
                if (sql.includes('where')) {
                    const whereMatch = sql.match(/where\s+(\w+)\s*=\s*'([^']+)'/);
                    if (whereMatch) {
                        args.filters = [{
                            column: whereMatch[1],
                            operator: 'eq',
                            value: whereMatch[2]
                        }];
                    }
                }
                
                // Detecta LIMIT
                const limitMatch = sql.match(/limit\s+(\d+)/);
                if (limitMatch) {
                    args.limit = parseInt(limitMatch[1]);
                }
                
                return await this.queryRecords(args);
            }
        }
        
        throw new Error('Query SQL não suportada pelo fallback');
    }

    applyFilters(query, filters) {
        filters.forEach(filter => {
            const { column, operator, value } = filter;

            switch (operator) {
                case 'eq':
                    query = query.eq(column, value);
                    break;
                case 'neq':
                    query = query.neq(column, value);
                    break;
                case 'gt':
                    query = query.gt(column, value);
                    break;
                case 'gte':
                    query = query.gte(column, value);
                    break;
                case 'lt':
                    query = query.lt(column, value);
                    break;
                case 'lte':
                    query = query.lte(column, value);
                    break;
                case 'like':
                    query = query.like(column, value);
                    break;
                case 'ilike':
                    query = query.ilike(column, value);
                    break;
                default:
                    console.warn(`Operador não suportado: ${operator}`);
            }
        });

        return query;
    }

    async run() {
        const transport = new StdioServerTransport();
        await this.server.connect(transport);
        console.error('Servidor MCP Supabase iniciado');
    }
}

// Inicia o servidor se executado diretamente
if (require.main === module) {
    const server = new SupabaseMCPServer();
    server.run().catch(console.error);
}

module.exports = SupabaseMCPServer;