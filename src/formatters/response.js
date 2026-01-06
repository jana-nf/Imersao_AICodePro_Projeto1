class ResponseFormatter {
    constructor() {
        this.maxMessageLength = 4000; // Limite do WhatsApp
        this.maxRecordsToShow = 20;
    }

    format(aiResponse) {
        try {
            // O novo agente Claude já retorna a resposta formatada
            if (aiResponse.response) {
                return this.truncateMessage(aiResponse.response);
            }

            // Fallback para formato antigo
            const { analysis, result, intent } = aiResponse;
            const operation = analysis?.operation || intent?.operation;

            if (!result.success) {
                return this.formatError(result, analysis || intent);
            }

            switch (operation) {
                case 'list_tables':
                    return this.formatTablesList(result, analysis || intent);
                
                case 'count':
                case 'count_records':
                    return this.formatCount(result, analysis || intent);
                
                case 'list':
                case 'list_records':
                case 'filter':
                case 'search':
                    return this.formatRecords(result, analysis || intent);
                
                default:
                    return this.formatGeneric(result, analysis || intent);
            }

        } catch (error) {
            console.error('❌ Erro ao formatar resposta:', error);
            return '🤖 Desculpe, ocorreu um erro ao formatar a resposta.';
        }
    }

    formatTablesList(result, intent) {
        const { data, count } = result;
        
        if (!data || data.length === 0) {
            return '📊 Nenhuma tabela encontrada no banco de dados.';
        }

        let message = `📊 *Tabelas Disponíveis* (${count} encontradas)\n\n`;
        
        data.forEach((table, index) => {
            message += `${index + 1}. *${table.table_name}*`;
            
            if (table.columns && table.columns.length > 0) {
                const columnCount = table.columns.length;
                message += ` (${columnCount} colunas)`;
                
                // Mostra algumas colunas principais
                if (columnCount <= 5) {
                    message += `\n   � ${table.columns.join(', ')}`;
                } else {
                    message += `\n   � ${table.columns.slice(0, 3).join(', ')}... (+${columnCount - 3} mais)`;
                }
            }
            
            message += '\n\n';
        });

        message += '💡 *Dica:* Você pode perguntar sobre qualquer uma dessas tabelas!';
        
        return this.truncateMessage(message);
    }

    formatCount(result, intent) {
        const { data } = result;
        const tableName = intent.table;
        const count = data.count;

        let message = `📊 *Contagem - ${tableName}*\n\n`;
        message += `🔢 Total de registros: *${count.toLocaleString('pt-BR')}*\n\n`;

        if (intent.filters && intent.filters.length > 0) {
            message += '🔍 *Filtros aplicados:*\n';
            intent.filters.forEach(filter => {
                message += `• ${filter.column} ${this.getOperatorSymbol(filter.operator)} ${filter.value}\n`;
            });
            message += '\n';
        }

        if (count === 0) {
            message += '💡 Nenhum registro encontrado com os critérios especificados.';
        } else if (count === 1) {
            message += '💡 Encontrado apenas 1 registro.';
        } else {
            message += `💡 Use "mostra os ${Math.min(10, count)} ${tableName}" para ver os registros.`;
        }

        return message;
    }

    formatRecords(result, intent) {
        const { data, count } = result;
        const tableName = intent.table;

        if (!data || data.length === 0) {
            let message = `📋 *${tableName}* - Nenhum registro encontrado\n\n`;
            
            if (intent.filters && intent.filters.length > 0) {
                message += '🔍 *Filtros aplicados:*\n';
                intent.filters.forEach(filter => {
                    message += `• ${filter.column} ${this.getOperatorSymbol(filter.operator)} ${filter.value}\n`;
                });
                message += '\n';
            }
            
            message += '💡 Tente ajustar os filtros ou verificar se os dados existem.';
            return message;
        }

        let message = `📋 *${tableName}* (${count} ${count === 1 ? 'registro' : 'registros'})\n\n`;

        // Adiciona informações sobre filtros
        if (intent.filters && intent.filters.length > 0) {
            message += '🔍 *Filtros:* ';
            const filterTexts = intent.filters.map(filter => 
                `${filter.column} ${this.getOperatorSymbol(filter.operator)} ${filter.value}`
            );
            message += filterTexts.join(', ') + '\n\n';
        }

        // Limita o número de registros mostrados
        const recordsToShow = Math.min(data.length, this.maxRecordsToShow);
        
        for (let i = 0; i < recordsToShow; i++) {
            const record = data[i];
            message += `*${i + 1}.* ${this.formatRecord(record)}\n\n`;
        }

        // Aviso se há mais registros
        if (data.length > this.maxRecordsToShow) {
            message += `⚠️ Mostrando apenas ${this.maxRecordsToShow} de ${data.length} registros.\n`;
            message += `💡 Use filtros mais específicos para refinar a busca.`;
        }

        return this.truncateMessage(message);
    }

    formatRecord(record) {
        const keys = Object.keys(record);
        const importantFields = this.getImportantFields(keys);
        
        let recordText = '';
        
        // Mostra campos importantes primeiro
        importantFields.forEach(key => {
            const value = this.formatValue(record[key]);
            if (value) {
                recordText += `${this.formatFieldName(key)}: ${value}\n`;
            }
        });

        // Mostra outros campos se houver espaço
        const otherFields = keys.filter(key => !importantFields.includes(key));
        if (otherFields.length > 0 && recordText.length < 200) {
            otherFields.slice(0, 3).forEach(key => {
                const value = this.formatValue(record[key]);
                if (value) {
                    recordText += `${this.formatFieldName(key)}: ${value}\n`;
                }
            });
        }

        return recordText.trim();
    }

    getImportantFields(keys) {
        const priorityFields = [
            'name', 'nome', 'title', 'titulo',
            'email', 'phone', 'telefone',
            'id', 'code', 'codigo',
            'status', 'type', 'tipo',
            'created_at', 'updated_at'
        ];

        const important = [];
        
        // Adiciona campos prioritários que existem
        priorityFields.forEach(field => {
            const found = keys.find(key => 
                key.toLowerCase().includes(field.toLowerCase())
            );
            if (found && !important.includes(found)) {
                important.push(found);
            }
        });

        // Adiciona outros campos até um máximo
        const remaining = keys.filter(key => !important.includes(key));
        important.push(...remaining.slice(0, 5 - important.length));

        return important;
    }

    formatFieldName(fieldName) {
        // Converte snake_case para formato legível
        return fieldName
            .replace(/_/g, ' ')
            .replace(/\b\w/g, l => l.toUpperCase());
    }

    formatValue(value) {
        if (value === null || value === undefined) {
            return '';
        }

        if (typeof value === 'string') {
            // Trunca strings muito longas
            return value.length > 50 ? value.substring(0, 47) + '...' : value;
        }

        if (typeof value === 'number') {
            return value.toLocaleString('pt-BR');
        }

        if (typeof value === 'boolean') {
            return value ? '✅ Sim' : '❌ Não';
        }

        if (value instanceof Date || (typeof value === 'string' && value.match(/^\d{4}-\d{2}-\d{2}/))) {
            try {
                const date = new Date(value);
                return date.toLocaleDateString('pt-BR');
            } catch {
                return value;
            }
        }

        return String(value);
    }

    getOperatorSymbol(operator) {
        const symbols = {
            'eq': '=',
            'neq': '≠',
            'gt': '>',
            'gte': '≥',
            'lt': '<',
            'lte': '≤',
            'like': 'contém',
            'ilike': 'contém',
            'in': 'em',
            'is': 'é'
        };

        return symbols[operator] || operator;
    }

    formatError(result, intent) {
        let message = '❌ *Erro ao processar solicitação*\n\n';
        
        if (intent && intent.explanation) {
            message += `🎯 *Tentativa:* ${intent.explanation}\n\n`;
        }

        message += `🔍 *Erro:* ${result.error || result.message}\n\n`;

        // Sugestões baseadas no tipo de erro
        if (result.error && result.error.includes('relation') && result.error.includes('does not exist')) {
            message += '💡 *Sugestão:* A tabela pode não existir. Tente:\n';
            message += '• "quantas tabelas tem?" para ver tabelas disponíveis\n';
            message += '• Verificar se o nome da tabela está correto';
        } else if (result.error && result.error.includes('column')) {
            message += '💡 *Sugestão:* Problema com coluna. Tente:\n';
            message += '• Verificar se o nome da coluna está correto\n';
            message += '• Usar termos mais simples na busca';
        } else {
            message += '💡 *Sugestão:* Tente reformular sua pergunta ou use:\n';
            message += '• "quantas tabelas tem?" para ver o que está disponível\n';
            message += '• Perguntas mais simples como "quantos clientes tem?"';
        }

        return message;
    }

    formatGeneric(result, intent) {
        let message = '🤖 *Resultado*\n\n';
        
        if (intent && intent.explanation) {
            message += `${intent.explanation}\n\n`;
        }

        if (result.message) {
            message += `📋 ${result.message}\n\n`;
        }

        if (result.data) {
            message += `📊 Dados: ${JSON.stringify(result.data, null, 2)}`;
        }

        return this.truncateMessage(message);
    }

    truncateMessage(message) {
        if (message.length <= this.maxMessageLength) {
            return message;
        }

        const truncated = message.substring(0, this.maxMessageLength - 100);
        const lastNewline = truncated.lastIndexOf('\n');
        
        return truncated.substring(0, lastNewline) + 
               '\n\n⚠️ Mensagem truncada devido ao limite do WhatsApp.';
    }
}

module.exports = ResponseFormatter;