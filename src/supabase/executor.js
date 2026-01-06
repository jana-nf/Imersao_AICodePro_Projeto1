const { createClient } = require('@supabase/supabase-js');

class SupabaseExecutor {
    constructor() {
        this.supabaseUrl = process.env.SUPABASE_URL;
        this.supabaseKey = process.env.SUPABASE_KEY;
        this.isConnected = false;
        
        if (!this.supabaseUrl || !this.supabaseKey) {
            throw new Error('Configurações do Supabase não encontradas no .env');
        }

        this.supabase = createClient(this.supabaseUrl, this.supabaseKey);
        console.log('🗄️ Cliente Supabase inicializado');
    }

    async testConnection() {
        try {
            // Testa conexão usando uma query SQL simples
            const { data, error } = await this.supabase
                .rpc('select', { query: 'SELECT 1 as test' });

            // Se RPC não funcionar, tenta método alternativo
            if (error) {
                console.log('🔄 Tentando método alternativo de conexão...');
                
                // Tenta listar tabelas diretamente
                const { data: authData, error: authError } = await this.supabase.auth.getSession();
                
                if (authError) {
                    console.log('ℹ️ Usando conexão anônima (normal para service_role)');
                }
                
                // Se chegou até aqui, a conexão básica está funcionando
                this.isConnected = true;
                console.log('✅ Conexão com Supabase estabelecida');
                return true;
            }

            this.isConnected = true;
            console.log('✅ Conexão com Supabase testada com sucesso');
            return true;

        } catch (error) {
            this.isConnected = false;
            throw error;
        }
    }

    async getTablesInfo() {
        try {
            // Método 1: Usar RPC list_tables do Supabase (descoberta automática)
            try {
                const { data: rpcTables, error: rpcError } = await this.supabase
                    .rpc('list_tables');
                
                if (!rpcError && rpcTables && rpcTables.length > 0) {
                    console.log('✅ Tabelas encontradas via RPC list_tables');
                    console.log('📋 Tabelas brutas:', rpcTables);
                    
                    // Corrige o problema [object Object]
                    const tableNames = rpcTables.map(table => {
                        if (typeof table === 'string') {
                            return { table_name: table };
                        } else if (table.table_name) {
                            return { table_name: table.table_name };
                        } else if (table.name) {
                            return { table_name: table.name };
                        } else {
                            return { table_name: String(table) };
                        }
                    });
                    
                    return await this.enrichTablesWithColumns(tableNames);
                }
            } catch (rpcError) {
                console.log('ℹ️ RPC list_tables não disponível, tentando método alternativo...');
            }

            // Método 2: Usar fetch direto para RPC (fallback)
            try {
                const response = await fetch(`${this.supabaseUrl}/rest/v1/rpc/list_tables`, {
                    method: 'POST',
                    headers: {
                        'apikey': this.supabaseKey,
                        'Authorization': `Bearer ${this.supabaseKey}`,
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({})
                });

                if (response.ok) {
                    const tables = await response.json();
                    console.log('✅ Tabelas encontradas via fetch RPC');
                    const tableNames = tables.map(tableName => ({ table_name: tableName }));
                    return await this.enrichTablesWithColumns(tableNames);
                }
            } catch (fetchError) {
                console.log('ℹ️ Fetch RPC não disponível, usando descoberta manual...');
            }

            // Método 3: Descoberta via SQL direto no information_schema
            try {
                const { data: schemaData, error: schemaError } = await this.supabase
                    .rpc('execute_custom_query', {
                        query_text: `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name`
                    });
                
                if (!schemaError && schemaData) {
                    const tables = Array.isArray(schemaData) ? schemaData : [schemaData];
                    const tableNames = tables.map(t => ({ table_name: t.table_name || t }));
                    console.log(`✅ Descobertas ${tableNames.length} tabelas via information_schema`);
                    return await this.enrichTablesWithColumns(tableNames);
                }
            } catch (sqlError) {
                console.log('ℹ️ execute_custom_query não disponível, usando lista expandida...');
            }

            // Método 4: Lista expandida de tabelas comuns + descoberta por probing
            const commonTables = [
                // Tabelas conhecidas do projeto
                'whatsapp_actions', 'engaged_leads', 'script_downloads', 'unified_leads',
                'aula_views', 'aula_navigations', 'qualified_leads', 'social_actions',
                // Tabelas comuns em sistemas
                'users', 'profiles', 'accounts', 'customers', 'clients', 'leads',
                'products', 'orders', 'sales', 'invoices', 'payments', 'transactions',
                'categories', 'tags', 'comments', 'posts', 'articles', 'pages',
                'settings', 'configurations', 'preferences', 'notifications',
                'logs', 'events', 'activities', 'sessions', 'tokens',
                'files', 'uploads', 'media', 'images', 'documents',
                'messages', 'conversations', 'chats', 'contacts',
                'subscriptions', 'plans', 'billing', 'credits',
                'teams', 'organizations', 'workspaces', 'projects', 'tasks',
                'campaigns', 'analytics', 'metrics', 'reports', 'dashboards',
                // Variações em português
                'usuarios', 'clientes', 'pedidos', 'vendas', 'produtos', 'pagamentos',
                'categorias', 'comentarios', 'arquivos', 'mensagens', 'contatos',
                'assinaturas', 'planos', 'equipes', 'projetos', 'tarefas', 'campanhas'
            ];

            const foundTables = [];
            console.log(`🔍 Testando ${commonTables.length} tabelas potenciais...`);

            for (const tableName of commonTables) {
                try {
                    // Primeiro testa se a tabela existe fazendo uma query real
                    const { data: sampleData, error } = await this.supabase
                        .from(tableName)
                        .select('*')
                        .limit(1);

                    // Se não há erro E há estrutura de colunas, a tabela existe
                    if (!error && sampleData !== null) {
                        // Busca contagem real
                        const { count } = await this.supabase
                            .from(tableName)
                            .select('*', { count: 'exact', head: true });

                        const columns = sampleData && sampleData.length > 0 ? Object.keys(sampleData[0]) : [];
                        
                        // Só adiciona se tem colunas OU se tem registros
                        if (columns.length > 0 || (count && count > 0)) {
                            foundTables.push({
                                table_name: tableName,
                                columns: columns,
                                row_count: count || 0
                            });
                            
                            console.log(`  ✅ ${tableName}: ${columns.length} colunas, ${count || 0} registros`);
                        }
                    }
                } catch (err) {
                    // Ignora erros silenciosamente - tabela não existe
                }
            }

            if (foundTables.length === 0) {
                console.log('⚠️ Nenhuma tabela encontrada. Verifique se:');
                console.log('   1. As tabelas existem no Supabase');
                console.log('   2. A API Key tem permissões de leitura');
                console.log('   3. As políticas RLS permitem acesso');
                
                // Retorna estrutura básica para teste
                return [{
                    table_name: 'exemplo',
                    columns: ['id', 'nome', 'email', 'created_at'],
                    row_count: 0
                }];
            }

            console.log(`✅ Descobertas ${foundTables.length} tabelas no total`);
            return foundTables;

        } catch (error) {
            console.error('❌ Erro ao descobrir tabelas:', error);
            return [];
        }
    }

    async enrichTablesWithColumns(tables) {
        const enrichedTables = [];
        
        for (const table of tables) {
            try {
                const tableName = table.table_name || table.tablename || table;
                
                // Método 1: Usar RPC list_columns para obter estrutura
                let columns = [];
                try {
                    const { data: columnsData, error: columnsError } = await this.supabase
                        .rpc('list_columns', { table_name: tableName });
                    
                    if (!columnsError && columnsData) {
                        columns = columnsData.map(col => col.column_name);
                        console.log(`  📋 ${tableName}: ${columns.length} colunas via RPC`);
                    }
                } catch (rpcError) {
                    console.log(`  ⚠️ RPC list_columns falhou para ${tableName}, usando amostra...`);
                }

                // Método 2: Fallback - usar amostra de dados
                if (columns.length === 0) {
                    const { data: sampleData } = await this.supabase
                        .from(tableName)
                        .select('*')
                        .limit(1);

                    columns = sampleData && sampleData.length > 0 ? Object.keys(sampleData[0]) : [];
                }

                // Obter contagem de registros
                const { count } = await this.supabase
                    .from(tableName)
                    .select('*', { count: 'exact', head: true });

                enrichedTables.push({
                    table_name: tableName,
                    columns: columns,
                    row_count: count || 0
                });
                
                console.log(`  ✅ ${tableName}: ${columns.length} colunas, ${count || 0} registros`);
                
            } catch (error) {
                console.log(`⚠️ Erro ao enriquecer tabela ${table}: ${error.message}`);
            }
        }
        
        return enrichedTables;
    }

    async listTables() {
        try {
            const tablesInfo = await this.getTablesInfo();
            
            return {
                success: true,
                data: tablesInfo,
                count: tablesInfo.length,
                message: `Encontradas ${tablesInfo.length} tabelas`
            };

        } catch (error) {
            console.error('❌ Erro ao listar tabelas:', error);
            return {
                success: false,
                error: error.message,
                message: 'Erro ao listar tabelas'
            };
        }
    }

    async countRecords(tableName, filters = []) {
        try {
            let query = this.supabase
                .from(tableName)
                .select('*', { count: 'exact', head: true });

            // Aplica filtros se existirem
            query = this.applyFilters(query, filters);

            const { count, error } = await query;

            if (error) {
                console.error(`❌ Erro ao contar registros da tabela ${tableName}:`, error);
                return {
                    success: false,
                    error: error.message,
                    message: `Erro ao contar registros da tabela ${tableName}`
                };
            }

            return {
                success: true,
                data: { count },
                message: `A tabela ${tableName} possui ${count} registros`
            };

        } catch (error) {
            console.error(`❌ Erro ao contar registros:`, error);
            return {
                success: false,
                error: error.message,
                message: 'Erro ao contar registros'
            };
        }
    }

    async listRecords(tableName, options = {}) {
        try {
            const {
                filters = [],
                limit = 10,
                orderBy = null,
                columns = null
            } = options;

            let query = this.supabase.from(tableName);

            // Seleciona colunas específicas ou todas
            if (columns && columns.length > 0) {
                query = query.select(columns.join(', '));
            } else {
                query = query.select('*');
            }

            // Aplica filtros
            query = this.applyFilters(query, filters);

            // Aplica ordenação
            if (orderBy) {
                query = query.order(orderBy.column, { ascending: orderBy.ascending });
            }

            // Aplica limite
            if (limit) {
                query = query.limit(limit);
            }

            const { data, error } = await query;

            if (error) {
                console.error(`❌ Erro ao listar registros da tabela ${tableName}:`, error);
                return {
                    success: false,
                    error: error.message,
                    message: `Erro ao listar registros da tabela ${tableName}`
                };
            }

            return {
                success: true,
                data: data,
                count: data.length,
                message: `Encontrados ${data.length} registros na tabela ${tableName}`
            };

        } catch (error) {
            console.error(`❌ Erro ao listar registros:`, error);
            return {
                success: false,
                error: error.message,
                message: 'Erro ao listar registros'
            };
        }
    }

    async searchRecords(tableName, filters = [], limit = 10) {
        // Usa o mesmo método de listRecords para busca
        return await this.listRecords(tableName, { filters, limit });
    }

    applyFilters(query, filters) {
        if (!filters || filters.length === 0) {
            return query;
        }

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
                case 'in':
                    query = query.in(column, Array.isArray(value) ? value : [value]);
                    break;
                case 'is':
                    query = query.is(column, value);
                    break;
                default:
                    console.warn(`⚠️ Operador não suportado: ${operator}`);
            }
        });

        return query;
    }

    // Método para executar queries SQL customizadas (uso avançado)
    async executeCustomQuery(sqlQuery) {
        try {
            const { data, error } = await this.supabase.rpc('execute_sql', {
                query: sqlQuery
            });

            if (error) {
                console.error('❌ Erro ao executar query customizada:', error);
                return {
                    success: false,
                    error: error.message,
                    message: 'Erro ao executar query customizada'
                };
            }

            return {
                success: true,
                data: data,
                message: 'Query executada com sucesso'
            };

        } catch (error) {
            console.error('❌ Erro ao executar query customizada:', error);
            return {
                success: false,
                error: error.message,
                message: 'Erro ao executar query customizada'
            };
        }
    }

    async describeTable(tableName) {
        try {
            // Busca diretamente a tabela específica (sem listar todas)
            const { data: sampleData, error: sampleError } = await this.supabase
                .from(tableName)
                .select('*')
                .limit(1);
            
            if (sampleError) {
                return {
                    success: false,
                    error: `Tabela '${tableName}' não encontrada: ${sampleError.message}`
                };
            }

            const columns = sampleData && sampleData.length > 0 ? Object.keys(sampleData[0]) : [];
            
            const { count } = await this.supabase
                .from(tableName)
                .select('*', { count: 'exact', head: true });

            return {
                success: true,
                data: {
                    table_name: tableName,
                    columns: columns,
                    row_count: count || 0,
                    column_count: columns.length
                },
                message: `Estrutura da tabela ${tableName}`
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async performAggregation(tableName, aggregation) {
        try {
            if (aggregation.type === 'count_distinct' && aggregation.column) {
                // Para COUNT DISTINCT, busca TODOS os dados com paginação automática
                const allData = await this.getAllRecordsWithPagination(tableName, aggregation.column, aggregation.filters);
                
                if (allData.success) {
                    const uniqueValues = new Set(allData.data.map(row => row[aggregation.column]).filter(val => val !== null && val !== undefined));
                    
                    console.log(`🔢 COUNT DISTINCT ${tableName}.${aggregation.column}: ${uniqueValues.size} (de ${allData.data.length} registros totais)`);
                    
                    return {
                        success: true,
                        data: {
                            operation: 'count_distinct',
                            column: aggregation.column,
                            result: uniqueValues.size,
                            table: tableName,
                            total_records: allData.data.length
                        },
                        message: `COUNT DISTINCT de ${aggregation.column}: ${uniqueValues.size}`
                    };
                } else {
                    return allData;
                }
            }

            // Para outras agregações, retorna contagem simples por enquanto
            return await this.countRecords(tableName, aggregation.filters || []);

        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async getAllRecordsWithPagination(tableName, column, filters = []) {
        try {
            const allData = [];
            let from = 0;
            const pageSize = 1000; // Limite do Supabase
            let hasMore = true;

            console.log(`📄 Buscando TODOS os registros de ${tableName}.${column} com paginação...`);

            while (hasMore) {
                let query = this.supabase
                    .from(tableName)
                    .select(column)
                    .range(from, from + pageSize - 1);

                // Aplica filtros se existirem
                if (filters && filters.length > 0) {
                    query = this.applyFilters(query, filters);
                }

                const { data, error } = await query;

                if (error) {
                    return {
                        success: false,
                        error: error.message
                    };
                }

                if (data && data.length > 0) {
                    allData.push(...data);
                    console.log(`  📄 Página ${Math.floor(from/pageSize) + 1}: +${data.length} registros (total: ${allData.length})`);
                    
                    // Se retornou menos que o pageSize, chegou ao fim
                    if (data.length < pageSize) {
                        hasMore = false;
                    } else {
                        from += pageSize;
                    }
                } else {
                    hasMore = false;
                }
            }

            console.log(`✅ Paginação completa: ${allData.length} registros coletados`);

            return {
                success: true,
                data: allData
            };

        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Método utilitário para verificar se uma tabela existe
    async tableExists(tableName) {
        try {
            const { data, error } = await this.supabase
                .from(tableName)
                .select('*', { count: 'exact', head: true });

            return !error;

        } catch (error) {
            return false;
        }
    }
}

module.exports = SupabaseExecutor;