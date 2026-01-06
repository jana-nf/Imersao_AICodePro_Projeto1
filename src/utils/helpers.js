/**
 * Utilitários gerais para o sistema WhatsApp + IA + Supabase
 */

class Helpers {
    /**
     * Valida se uma string é um número de telefone válido
     */
    static isValidPhoneNumber(phone) {
        // Remove caracteres não numéricos
        const cleanPhone = phone.replace(/\D/g, '');
        
        // Verifica se tem entre 10 e 15 dígitos (padrão internacional)
        return cleanPhone.length >= 10 && cleanPhone.length <= 15;
    }

    /**
     * Formata número de telefone para padrão WhatsApp
     */
    static formatPhoneForWhatsApp(phone) {
        const cleanPhone = phone.replace(/\D/g, '');
        
        // Se não tem código do país, adiciona 55 (Brasil)
        if (cleanPhone.length === 11 && cleanPhone.startsWith('11')) {
            return `55${cleanPhone}@c.us`;
        } else if (cleanPhone.length === 10) {
            return `5511${cleanPhone}@c.us`;
        } else if (cleanPhone.length === 13 && cleanPhone.startsWith('55')) {
            return `${cleanPhone}@c.us`;
        }
        
        return `${cleanPhone}@c.us`;
    }

    /**
     * Sanitiza texto para evitar problemas de formatação
     */
    static sanitizeText(text) {
        if (!text || typeof text !== 'string') return '';
        
        return text
            .replace(/[*_~`]/g, '') // Remove caracteres de formatação Markdown
            .replace(/\n{3,}/g, '\n\n') // Limita quebras de linha consecutivas
            .trim();
    }

    /**
     * Trunca texto mantendo palavras inteiras
     */
    static truncateText(text, maxLength = 100) {
        if (!text || text.length <= maxLength) return text;
        
        const truncated = text.substring(0, maxLength);
        const lastSpace = truncated.lastIndexOf(' ');
        
        return lastSpace > 0 ? truncated.substring(0, lastSpace) + '...' : truncated + '...';
    }

    /**
     * Converte objeto para string formatada
     */
    static objectToString(obj, indent = 0) {
        if (obj === null || obj === undefined) return 'null';
        
        if (typeof obj !== 'object') return String(obj);
        
        if (Array.isArray(obj)) {
            return obj.map(item => this.objectToString(item, indent)).join(', ');
        }
        
        const spaces = '  '.repeat(indent);
        const entries = Object.entries(obj)
            .filter(([key, value]) => value !== null && value !== undefined)
            .map(([key, value]) => {
                const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                const formattedValue = typeof value === 'object' 
                    ? this.objectToString(value, indent + 1)
                    : String(value);
                return `${spaces}${formattedKey}: ${formattedValue}`;
            });
        
        return entries.join('\n');
    }

    /**
     * Detecta tipo de consulta baseado no texto
     */
    static detectQueryType(text) {
        const lowerText = text.toLowerCase();
        
        // Padrões para diferentes tipos de consulta
        const patterns = {
            count: /^(quantos?|total|contar|número)/,
            list: /^(mostra|lista|exibe|ver|últimos?|primeiros?)/,
            search: /^(busca|procura|encontra|acha)/,
            filter: /\b(com|que|onde|tendo|maior|menor|igual|contém)\b/,
            tables: /^(que|quais|quantas).*(tabelas?|bases?)/
        };

        for (const [type, pattern] of Object.entries(patterns)) {
            if (pattern.test(lowerText)) {
                return type;
            }
        }

        return 'unknown';
    }

    /**
     * Extrai números de um texto
     */
    static extractNumbers(text) {
        const matches = text.match(/\d+/g);
        return matches ? matches.map(Number) : [];
    }

    /**
     * Formata data para exibição
     */
    static formatDate(date, format = 'pt-BR') {
        if (!date) return '';
        
        try {
            const dateObj = new Date(date);
            
            if (format === 'pt-BR') {
                return dateObj.toLocaleDateString('pt-BR', {
                    day: '2-digit',
                    month: '2-digit',
                    year: 'numeric'
                });
            }
            
            return dateObj.toISOString().split('T')[0];
        } catch (error) {
            return String(date);
        }
    }

    /**
     * Formata moeda brasileira
     */
    static formatCurrency(value) {
        if (typeof value !== 'number') return value;
        
        return new Intl.NumberFormat('pt-BR', {
            style: 'currency',
            currency: 'BRL'
        }).format(value);
    }

    /**
     * Gera ID único simples
     */
    static generateId() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2);
    }

    /**
     * Delay assíncrono
     */
    static async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Retry com backoff exponencial
     */
    static async retry(fn, maxAttempts = 3, baseDelay = 1000) {
        let lastError;
        
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return await fn();
            } catch (error) {
                lastError = error;
                
                if (attempt === maxAttempts) {
                    throw error;
                }
                
                const delay = baseDelay * Math.pow(2, attempt - 1);
                console.log(`Tentativa ${attempt} falhou, tentando novamente em ${delay}ms...`);
                await this.delay(delay);
            }
        }
        
        throw lastError;
    }

    /**
     * Valida estrutura de filtro
     */
    static validateFilter(filter) {
        if (!filter || typeof filter !== 'object') return false;
        
        const { column, operator, value } = filter;
        
        if (!column || !operator) return false;
        
        const validOperators = ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'like', 'ilike', 'in', 'is'];
        
        return validOperators.includes(operator);
    }

    /**
     * Limpa e valida nome de tabela
     */
    static sanitizeTableName(tableName) {
        if (!tableName || typeof tableName !== 'string') return null;
        
        // Remove caracteres especiais e espaços
        const clean = tableName
            .toLowerCase()
            .replace(/[^a-z0-9_]/g, '')
            .trim();
        
        // Verifica se é um nome válido
        if (clean.length === 0 || /^\d/.test(clean)) return null;
        
        return clean;
    }

    /**
     * Converte texto para slug
     */
    static textToSlug(text) {
        if (!text) return '';
        
        return text
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '') // Remove acentos
            .replace(/[^a-z0-9\s-]/g, '') // Remove caracteres especiais
            .replace(/\s+/g, '-') // Substitui espaços por hífens
            .replace(/-+/g, '-') // Remove hífens duplicados
            .trim();
    }

    /**
     * Verifica se é um JSON válido
     */
    static isValidJSON(str) {
        try {
            JSON.parse(str);
            return true;
        } catch {
            return false;
        }
    }

    /**
     * Log formatado com timestamp
     */
    static log(level, message, data = null) {
        const timestamp = new Date().toISOString();
        const logLevel = level.toUpperCase().padEnd(5);
        
        let logMessage = `[${timestamp}] ${logLevel} ${message}`;
        
        if (data) {
            logMessage += `\n${JSON.stringify(data, null, 2)}`;
        }
        
        console.log(logMessage);
    }
}

module.exports = Helpers;