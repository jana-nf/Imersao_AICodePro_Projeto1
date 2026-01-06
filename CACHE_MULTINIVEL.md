# Sistema de Cache Multinível - AI Data Scientist AICODEPRO

Este diagrama explica em detalhes o sistema de cache multinível implementado no projeto, que agora inclui o **Fast Path** como camada zero de otimização, seguido de cache em memória e cache persistente em arquivo.

```mermaid
flowchart TD
    %% Entrada da consulta
    query[Consulta do Usuário] --> fast_path{⚡ FAST PATH\nMETA_PATTERNS?}
    
    %% ⚡ Fast Path - Camada 0 (NOVO)
    fast_path -->|"'o que você pode fazer?'\n'oi', 'ajuda', 'status'"| identity[SYSTEM_IDENTITY\nRespostas Predefinidas]
    identity --> instant[⚡ Retorno Instantâneo\n< 50ms]
    
    %% Consultas de dados seguem para cache
    fast_path -->|"Consulta de dados"| hash[Geração de Hash\nda Consulta]
    
    %% Verificação de cache
    hash --> check_mem{Cache\nem Memória?}
    
    %% Fluxo de cache em memória (rápido)
    check_mem -->|Sim| mem_hit[Cache Hit\nMemória]
    mem_hit --> mem_return[Retorno Imediato\n<5ms]
    
    %% Verificação em cache de arquivo
    check_mem -->|Não| check_file{Cache\nem Arquivo?}
    
    %% Fluxo de cache em arquivo (médio)
    check_file -->|Sim| file_hit[Cache Hit\nArquivo]
    file_hit --> file_read[Leitura do Arquivo\n~50ms]
    file_read --> update_mem[Atualizar\nCache em Memória]
    update_mem --> file_return[Retorno\n~50-100ms]
    
    %% Fluxo sem cache (lento)
    check_file -->|Não| miss[Cache Miss]
    miss --> full_process[Processamento Completo\n- Análise de intenção\n- Geração de SQL\n- Consulta ao banco\n- Formatação\n~2-3 segundos]
    
    %% Armazenamento em cache após processamento
    full_process --> result[Resultado Final]
    result --> store_mem[Armazenar em\nCache de Memória]
    result --> store_file[Armazenar em\nCache de Arquivo]
    
    %% Retornos
    mem_return --> response[Resposta ao Usuário]
    file_return --> response
    store_file --> response
    
    %% Políticas de TTL
    subgraph "Políticas de TTL Dinâmico"
        ttl_calc[Cálculo de TTL Baseado em:]
        
        factors[Fatores de Ajuste:
        - Complexidade da consulta
        - Frequência de uso
        - Volatilidade dos dados
        - Hora do dia]
        
        ttl_calc --> mem_ttl[Cache em Memória:
        TTL Curto (5-30min)]
        
        ttl_calc --> file_ttl[Cache em Arquivo:
        TTL Longo (1-24h)]
        
        factors --> ttl_calc
    end
    
    result --> ttl_calc
    
    %% Invalidação de Cache
    subgraph "Invalidação de Cache"
        triggers[Gatilhos de Invalidação:
        - Atualização de dados
        - Mudança de schema
        - Forçado pelo usuário]
        
        triggers --> invalidate[Invalidador de Cache]
        
        invalidate --> clear_mem[Limpar Cache\nde Memória]
        invalidate --> clear_file[Limpar Cache\nde Arquivo]
    end
    
    %% Warm-up de Cache
    subgraph "Warm-up de Cache"
        startup[Inicialização\ndo Sistema]
        
        startup --> load_common[Carregar Consultas\nMais Comuns]
        
        load_common --> prefetch[Pré-carregar\nResultados]
        
        prefetch --> warm_mem[Preencher\nCache de Memória]
    end
    
    %% Estilos
    classDef input fill:#e3f2fd,stroke:#333,stroke-width:1px;
    classDef process fill:#f5f5f5,stroke:#333,stroke-width:1px;
    classDef decision fill:#e1f5fe,stroke:#333,stroke-width:1px;
    classDef cache fill:#e8f5e9,stroke:#333,stroke-width:1px;
    classDef output fill:#f9fbe7,stroke:#333,stroke-width:1px;
    classDef policy fill:#fff8e1,stroke:#333,stroke-width:1px;
    classDef invalidation fill:#ffebee,stroke:#333,stroke-width:1px;
    
    class query,hash input;
    class full_process,update_mem,file_read process;
    class check_mem,check_file decision;
    class mem_hit,file_hit,miss,store_mem,store_file,mem_return,file_return cache;
    class result,response output;
    class ttl_calc,factors,mem_ttl,file_ttl policy;
    class triggers,invalidate,clear_mem,clear_file invalidation;
    class startup,load_common,prefetch,warm_mem process;
```

## Detalhamento do Sistema de Cache Multinível

### Componentes Principais

0. **⚡ Fast Path (NOVO - Camada Zero)**
   - Detecta perguntas meta via `META_PATTERNS` (regex)
   - Responde via `SYSTEM_IDENTITY` predefinido
   - Acesso instantâneo (<50ms)
   - Não consulta banco de dados
   - Perguntas: "o que você pode fazer?", "oi", "ajuda", "status"

1. **Cache em Memória**
   - Armazenado diretamente na RAM do Node.js
   - Acesso ultra-rápido (<5ms)
   - Volátil (perdido em caso de reinicialização)
   - TTL curto (5-30 minutos)
   - Implementado com Map/Object em JavaScript

2. **Cache em Arquivo**
   - Armazenado em sistema de arquivos local
   - Acesso rápido (~50-100ms)
   - Persistente entre reinicializações
   - TTL longo (1-24 horas)
   - Implementado com sistema de arquivos JSON

### Fluxo de Processamento

1. **Verificação de Cache**
   - Gera hash único da consulta (considerando usuário, contexto, etc.)
   - Verifica primeiro no cache em memória (mais rápido)
   - Se não encontrado, verifica no cache em arquivo
   - Se não encontrado em nenhum lugar, processa a consulta completa

2. **Armazenamento em Cache**
   - Após processamento completo, armazena resultado em ambos os caches
   - Aplica TTL dinâmico baseado em vários fatores

3. **Invalidação Inteligente**
   - Monitora atualizações de dados para invalidar caches relacionados
   - Permite invalidação seletiva (apenas caches afetados)
   - Suporta invalidação manual para casos específicos

4. **Warm-up de Cache**
   - Na inicialização do sistema, pré-carrega consultas frequentes
   - Reduz latência para primeiros usuários após reinicialização
   - Prioriza consultas com base em histórico de uso

### Benefícios do Sistema

1. **Performance**
   - Redução de latência de 2-3 segundos para <5ms em consultas frequentes
   - Diminuição de 85% na carga do banco de dados
   - Suporte a 10x mais usuários simultâneos com mesma infraestrutura

2. **Resiliência**
   - Funcionamento mesmo em caso de falha temporária do banco de dados
   - Persistência de dados entre reinicializações do sistema
   - Degradação graceful em caso de problemas

3. **Economia de Recursos**
   - Redução significativa de consultas ao banco de dados
   - Menor consumo de CPU e memória em operações repetitivas
   - Economia em custos de infraestrutura
