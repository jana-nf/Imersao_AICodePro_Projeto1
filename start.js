#!/usr/bin/env node

/**
 * Script de inicialização rápida do sistema WhatsApp + IA + Supabase
 */

const fs = require('fs');
const path = require('path');

// Previne múltiplas execuções
if (global.startScriptRunning) {
    console.log('⚠️ Script de inicialização já está rodando!');
    process.exit(0);
}
global.startScriptRunning = true;

console.log('🚀 Sistema WhatsApp + IA + Supabase');
console.log('═'.repeat(50));

// Verificações básicas
console.log('🔍 Verificando configurações...');

// Verifica .env
if (!fs.existsSync('.env')) {
    console.error('❌ Arquivo .env não encontrado!');
    process.exit(1);
}

// Carrega variáveis
require('dotenv').config();

// Verifica variáveis essenciais
const required = ['OPENAI_API_KEY', 'SUPABASE_URL', 'SUPABASE_KEY'];
const missing = required.filter(v => !process.env[v]);

if (missing.length > 0) {
    console.error('❌ Variáveis obrigatórias não configuradas:', missing.join(', '));
    process.exit(1);
}

console.log('✅ Configurações OK');

// Verifica dependências
console.log('📦 Verificando dependências...');

try {
    require('whatsapp-web.js');
    require('@supabase/supabase-js');
    require('openai');
    require('express');
    console.log('✅ Dependências OK');
} catch (error) {
    console.error('❌ Execute: npm install');
    process.exit(1);
}

console.log('🎯 Iniciando sistema...\n');

// Instruções
console.log('📖 COMO USAR:');
console.log('─'.repeat(30));
console.log('1. 📱 Escaneie o QR Code com WhatsApp');
console.log('2. 💬 Envie mensagens como:');
console.log('   • "quantas tabelas tem?"');
console.log('   • "quantos leads tem?"');
console.log('   • "mostra os últimos 5 clientes"');
console.log('3. 🌐 Status: http://localhost:8080');
console.log('4. 🛑 Parar: Ctrl+C\n');

// Inicia o sistema principal apenas uma vez
try {
    require('./src/index.js');
} catch (error) {
    console.error('❌ Erro ao iniciar sistema:', error);
    process.exit(1);
}