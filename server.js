require('dotenv').config();
const express = require('express');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { Server } = require('socket.io');

// --- IMPORTAÇÃO DO MONGODB ---
const { MongoClient } = require('mongodb');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

// ========================================================
// ☁️ CONFIGURAÇÃO DA CHAVE MESTRA DO MONGODB (O COFRE)
// ========================================================
// A URI agora vem do arquivo .env — NUNCA coloque a senha direto no código!
const MONGO_URI = process.env.MONGO_URI;
if (!MONGO_URI) {
    console.error("❌ ERRO: variável MONGO_URI não encontrada!");
    console.error("   Crie um arquivo .env na raiz do projeto com a linha:");
    console.error('   MONGO_URI="sua_string_de_conexao_aqui"');
}
const mongoClient = new MongoClient(MONGO_URI);
let dbCofre; // Variável que guarda a conexão com a nuvem

// --- MEMÓRIA CENTRAL DO SISTEMA ---
let filaPacientes = []; 
let contadores = { 'RP': 1, 'R': 1, 'CP': 1, 'C': 1, 'AT': 1 };
let turnos = { 'REGULACAO': 'P', 'COMPLEXIDADE': 'P', 'AUTORIZACAO': 'P' };

// === CONTROLE DE FICHAS FÍSICAS ===
const fichasDesativadas = new Set(); 

// === RADAR DE OPERADORES ONLINE ===
const operadoresOnline = {}; 

let ultimosChamados = {
    'Regulação': [ { ficha: '---', nome: 'Nenhum' }, { ficha: '---', nome: 'Nenhum' } ],
    'Complexidade': [ { ficha: '---', nome: 'Nenhum' }, { ficha: '---', nome: 'Nenhum' } ],
    'Autorização': [ { ficha: '---', nome: 'Nenhum' }, { ficha: '---', nome: 'Nenhum' } ]
};

let filaDeEsperaTV = []; 
let tvFalando = false;   
let timerSegurancaTV = null;

const dadosPath = path.join(__dirname, 'dados.json');
const fsPromises = fs.promises;

app.get('/backup-dados', (req, res) => {
    const tokenEsperado = process.env.BACKUP_TOKEN;
    if (!tokenEsperado) {
        console.error("❌ BACKUP_TOKEN não configurado no .env — rota /backup-dados bloqueada por segurança.");
        return res.status(503).send('Backup indisponível: configure BACKUP_TOKEN no .env do servidor.');
    }
    if (req.query.token !== tokenEsperado) {
        return res.status(401).send('Não autorizado.');
    }
    res.download(dadosPath, `backup-regulacao-${getDataString()}.json`);
});

function getDataString() {
    return new Date().toISOString().slice(0, 10);
}

let atendimentosPorOperador = {};
let statusDia = { aberto: true, data: getDataString() };
let historicoAtendimentos = [];
let pendingWrite = null;
let lastWriteContent = null;

function obterEstadoAtual() {
    return { filaPacientes, contadores, turnos, ultimosChamados, statusDia, atendimentosPorOperador };
}

// === MOTOR DE INTELIGÊNCIA MATEMÁTICA ===
function calcularEstatisticasAbsenteismo() {
    const dadosPorDia = {};
    
    historicoAtendimentos.forEach(item => {
        if (!item.data) return;
        if (!dadosPorDia[item.data]) dadosPorDia[item.data] = { total: 0, faltas: 0, setores: {} };
        const setor = item.setor || 'Regulação';
        if (!dadosPorDia[item.data].setores[setor]) dadosPorDia[item.data].setores[setor] = { total: 0, faltas: 0 };
        
        if (item.resultado === 'atendido' || item.resultado === 'falta') {
            dadosPorDia[item.data].total++;
            dadosPorDia[item.data].setores[setor].total++;
        }
        if (item.resultado === 'falta') {
            dadosPorDia[item.data].faltas++;
            dadosPorDia[item.data].setores[setor].faltas++;
        }
    });
    
    const listaDatas = Object.keys(dadosPorDia);
    const processarMetricas = (arrayTaxas) => {
        const n = arrayTaxas.length;
        if (n === 0) return { media: 0, variancia: 0, desvioPadrao: 0 };
        const media = arrayTaxas.reduce((a, b) => a + b, 0) / n;
        if (n <= 1) return { media: Math.round(media), variancia: 0, desvioPadrao: 0 };
        const somaQuadrados = arrayTaxas.reduce((acc, val) => acc + Math.pow(val - media, 2), 0);
        const variancia = somaQuadrados / (n - 1);
        const desvioPadrao = Math.sqrt(variancia);
        return { media: Math.round(media), variancia: Math.round(variancia * 100) / 100, desvioPadrao: Math.round(desvioPadrao * 100) / 100 };
    };
    
    const taxasGerais = listaDatas.map(d => {
        const dia = dadosPorDia[d];
        return dia.total > 0 ? (dia.faltas / dia.total) * 100 : 0;
    });
    
    const estatisticas = { geral: processarMetricas(taxasGerais), setores: {} };
    const setoresDisponiveis = ['Regulação', 'Complexidade', 'Autorização'];
    setoresDisponiveis.forEach(setor => {
        const taxasSetor = [];
        listaDatas.forEach(d => {
            const diaSetor = dadosPorDia[d].setores[setor];
            if (diaSetor && diaSetor.total > 0) taxasSetor.push((diaSetor.faltas / diaSetor.total) * 100);
        });
        estatisticas.setores[setor] = processarMetricas(taxasSetor);
    });
    return estatisticas;
}

// ========================================================
// 💾 NOVO SISTEMA DE GRAVAÇÃO HÍBRIDA (JSON + MONGO)
// ========================================================
function salvarDados() {
    // Estado "vivo" do sistema — pequeno e limitado, seguro pra reenviar sempre.
    const dadosObj = {
        id_documento: 'backup_principal', // Identificador fixo para o Mongo
        filaPacientes, contadores, turnos, ultimosChamados,
        statusDia, atendimentosPorOperador
        // historicoAtendimentos NÃO entra aqui: ele já vive na coleção própria
        // 'historico_atendimentos' (gravado registro por registro, ver
        // 'registrar_conclusao_atendimento'). Incluí-lo aqui significava reenviar
        // o histórico inteiro (crescendo pra sempre) ao Mongo a cada ação —
        // essa foi a maior causa do consumo de banda excessivo no Render.
    };

    // No arquivo local (disco, não consome banda de rede) mantemos tudo junto,
    // pra facilitar backup/restauração manual em caso de necessidade.
    const dadosParaArquivo = { ...dadosObj, historicoAtendimentos };
    const dataString = JSON.stringify(dadosParaArquivo, null, 2);
    lastWriteContent = dataString;

    // 1. Grava no disco local imediatamente (A impressora não sofre atraso)
    if (!pendingWrite) {
        pendingWrite = fsPromises.writeFile(dadosPath, dataString, 'utf8')
            .catch((err) => console.error('❌ Erro local:', err))
            .then(() => {
                pendingWrite = null;
                if (lastWriteContent !== dataString) salvarDados();
            });
    }

    // 2. O Piloto Automático: Envia pro Atlas silenciosamente em segundo plano
    //    (agora só o estado vivo, sem o histórico — muito mais leve)
    if (dbCofre) {
        dbCofre.collection('estado_regulacao').updateOne(
            { id_documento: 'backup_principal' }, 
            { $set: dadosObj, $unset: { historicoAtendimentos: "" } }, 
            { upsert: true }
        ).catch(err => console.error("❌ Falha no envio invisível para a nuvem:", err));
    }

    return pendingWrite;
}

// ========================================================
// 🔄 NOVO SISTEMA DE BOOT (RESTAURAÇÃO AUTOMÁTICA)
// ========================================================
async function carregarDados() {
    try {
        // Tenta conectar no Mongo primeiro
        await mongoClient.connect();
        dbCofre = mongoClient.db('db_regulacao');
        console.log("☁️ Conectado com Sucesso ao Cofre Atlas!");

        // Busca o estado vivo e o histórico (agora em coleções separadas)
        const dadosNuvem = await dbCofre.collection('estado_regulacao').findOne({ id_documento: 'backup_principal' });
        let historicoNuvem = await dbCofre.collection('historico_atendimentos').find({}).toArray();

        // Migração única: se ainda existir histórico embutido no documento antigo
        // (formato anterior) e a coleção nova estiver vazia, migra pra lá.
        if (historicoNuvem.length === 0 && dadosNuvem && Array.isArray(dadosNuvem.historicoAtendimentos) && dadosNuvem.historicoAtendimentos.length > 0) {
            console.log(`📦 Migrando ${dadosNuvem.historicoAtendimentos.length} registros de histórico para a coleção própria...`);
            await dbCofre.collection('historico_atendimentos').insertMany(dadosNuvem.historicoAtendimentos.map(h => ({ ...h })));
            historicoNuvem = dadosNuvem.historicoAtendimentos;
            await dbCofre.collection('estado_regulacao').updateOne(
                { id_documento: 'backup_principal' },
                { $unset: { historicoAtendimentos: "" } }
            );
        }
        
        if (dadosNuvem) {
            console.log("✈️ Restaurando dados a partir da nuvem...");
            aplicarDadosNaMemoria({ ...dadosNuvem, historicoAtendimentos: historicoNuvem });
            
            // Força a atualização do JSON local para ficar igual à nuvem
            await fsPromises.writeFile(dadosPath, JSON.stringify({ ...dadosNuvem, historicoAtendimentos: historicoNuvem }, null, 2), 'utf8');
            return; // Se deu certo, sai da função
        }
    } catch (err) {
        console.log("⚠️ Nuvem indisponível no momento. Recorrendo ao armazenamento local.");
        console.log("🕵️ DETALHE DO ERRO MONGODB:", err.message); // <--- ADICIONE ESTA LINHA
    }
    // Fallback: Se não tem internet ou o Mongo falhou, lê o JSON local
    try {
        await fsPromises.access(dadosPath, fs.constants.F_OK);
        const raw = await fsPromises.readFile(dadosPath, 'utf8');
        aplicarDadosNaMemoria(JSON.parse(raw));
        console.log('✅ Dados carregados localmente do dados.json');
    } catch (err) {
        console.log("⚠️ Sistema iniciando 100% zerado. Nenhum backup encontrado.");
    }
}

function aplicarDadosNaMemoria(dados) {
    if (dados && Array.isArray(dados.filaPacientes)) filaPacientes = dados.filaPacientes;
    if (dados && typeof dados.contadores === 'object') contadores = dados.contadores;
    if (dados && typeof dados.turnos === 'object') turnos = Object.assign({ 'REGULACAO': 'P', 'COMPLEXIDADE': 'P', 'AUTORIZACAO': 'P' }, dados.turnos);
    if (dados && typeof dados.ultimosChamados === 'object') ultimosChamados = dados.ultimosChamados;
    if (dados && typeof dados.statusDia === 'object') statusDia = dados.statusDia;
    if (dados && typeof dados.atendimentosPorOperador === 'object') atendimentosPorOperador = dados.atendimentosPorOperador;
    if (dados && Array.isArray(dados.historicoAtendimentos)) historicoAtendimentos = dados.historicoAtendimentos;
    reconstruirContagens();
}

function reconstruirContagens() {
    atendimentosPorOperador = {};
    const hoje = getDataString(); 
    historicoAtendimentos.forEach(item => {
        if (item.resultado === 'atendido' && item.atendente && item.data === hoje) {
            atendimentosPorOperador[item.atendente] = (atendimentosPorOperador[item.atendente] || 0) + 1;
        }
    });
}

function calcularMediaPorSetor() {
    const hoje = getDataString();
    const setores = { REGULACAO: [], COMPLEXIDADE: [], AUTORIZACAO: [] };
    historicoAtendimentos.forEach(item => {
        if (item.resultado !== 'atendido' || !item.horaAtendimento) return;
        const dataAtendimento = item.horaAtendimento.slice(0, 10);
        if (dataAtendimento !== hoje) return;
        if (item.setor === 'Regulação') setores.REGULACAO.push(item.tempoEspera);
        if (item.setor === 'Complexidade') setores.COMPLEXIDADE.push(item.tempoEspera);
        if (item.setor === 'Autorização') setores.AUTORIZACAO.push(item.tempoEspera);
    });
    return {
        REGULACAO: setores.REGULACAO.length ? Math.round(setores.REGULACAO.reduce((a,b) => a+b,0) / setores.REGULACAO.length) : 0,
        COMPLEXIDADE: setores.COMPLEXIDADE.length ? Math.round(setores.COMPLEXIDADE.reduce((a,b) => a+b,0) / setores.COMPLEXIDADE.length) : 0,
        AUTORIZACAO: setores.AUTORIZACAO.length ? Math.round(setores.AUTORIZACAO.reduce((a,b) => a+b,0) / setores.AUTORIZACAO.length) : 0,
        totalAtendidosHoje: setores.REGULACAO.length + setores.COMPLEXIDADE.length + setores.AUTORIZACAO.length
    };
}

// Agrupa broadcasts de estado completo que aconteçam em rajada (várias ações
// em menos de 1.5s) em um só envio, evitando tráfego redundante pra todos os
// clientes conectados quando várias fichas são processadas em sequência rápida.
let emitirEstadoAgendado = false;
function emitirEstadoCompleto() {
    if (emitirEstadoAgendado) return;
    emitirEstadoAgendado = true;
    setTimeout(() => {
        emitirEstadoAgendado = false;
        io.emit('estado_servidor', obterEstadoAtual());
        io.emit('atualizar_media_setores', calcularMediaPorSetor());
        io.emit('atualizar_estatisticas_absenteismo', calcularEstatisticasAbsenteismo());
    }, 1500);
}

const mapeamentoSetores = {
    'RP': 'Regulação', 'R': 'Regulação', 'CP': 'Complexidade', 'C': 'Complexidade', 'AT': 'Autorização'
};

function realizarResetGeral() {
    clearTimeout(timerSegurancaTV);
    filaPacientes = []; filaDeEsperaTV = []; tvFalando = false; 
    contadores = { 'RP': 1, 'R': 1, 'CP': 1, 'C': 1, 'AT': 1 };
    turnos = { 'REGULACAO': 'P', 'COMPLEXIDADE': 'P', 'AUTORIZACAO': 'P' };
    ultimosChamados = {
        'Regulação': [ { ficha: '---', nome: 'Nenhum' }, { ficha: '---', nome: 'Nenhum' } ],
        'Complexidade': [ { ficha: '---', nome: 'Nenhum' }, { ficha: '---', nome: 'Nenhum' } ],
        'Autorização': [ { ficha: '---', nome: 'Nenhum' }, { ficha: '---', nome: 'Nenhum' } ]
    };
    statusDia = { aberto: true, data: getDataString() };
    io.emit('atualizar_fila', filaPacientes);
    io.emit('atualizar_painel_setores', ultimosChamados);
    enviarQuantitativosFila();
    io.emit('liberar_botoes_tv_livre');
    io.emit('limpar_tv'); 
    io.emit('sistema_resetado');
    emitirEstadoCompleto();
    salvarDados(); 
    console.log(`⏰ Mudança de turno concluída! Novo dia iniciado: ${statusDia.data}`);
}

function agendarResetMeiaNoite() {
    const agora = new Date();
    const meiaNoite = new Date();
    meiaNoite.setHours(24, 0, 0, 0); 
    const tempoAteMeiaNoite = meiaNoite.getTime() - agora.getTime();
    setTimeout(() => {
        realizarResetGeral();
        setInterval(realizarResetGeral, 24 * 60 * 60 * 1000);
    }, tempoAteMeiaNoite);
}

const URL_DO_SEU_SISTEMA = "https://painel-da-regulacao.onrender.com"; 

setInterval(() => {
    https.get(URL_DO_SEU_SISTEMA, (res) => {
        console.log("🔄 Ping enviado para manter o servidor acordado.");
    }).on('error', (err) => {
        console.log("❌ Erro no ping anti-cochilo:", err.message);
    });
}, 10 * 60 * 1000); 

function enviarQuantitativosFila() {
    const quantitativos = {
        'REGULACAO': filaPacientes.filter(p => p.setor === 'Regulação' && p.status === 'LOBBY').length,
        'COMPLEXIDADE': filaPacientes.filter(p => p.setor === 'Complexidade' && p.status === 'LOBBY').length,
        'AUTORIZACAO': filaPacientes.filter(p => p.setor === 'Autorização' && p.status === 'LOBBY').length
    };
    io.emit('atualizar_contagem_paineis', quantitativos);
}

function enfileirarChamadaTV(pacoteDeChamada) {
    if (tvFalando) {
        filaDeEsperaTV.push(pacoteDeChamada);
    } else {
        executarDisparoTV(pacoteDeChamada);
    }
}

function executarDisparoTV(pacoteDeChamada) {
    tvFalando = true;
    io.emit('bloqueio_tv_ocupada', pacoteDeChamada);
    io.emit('tocar_chamada_tv', pacoteDeChamada);
    clearTimeout(timerSegurancaTV);
    timerSegurancaTV = setTimeout(() => {
        liberarServidorEProximo();
    }, 7000);
}

function liberarServidorEProximo() {
    if (filaDeEsperaTV.length > 0) {
        const proximaChamada = filaDeEsperaTV.shift();
        executarDisparoTV(proximaChamada);
    } else {
        tvFalando = false;
        io.emit('liberar_botoes_tv_livre');
    }
}

function extrairFichaComRegra(setorNome, prefixoPadrao) {
    const turnoAtual = turnos[setorNome];
    const siglaAlvo = (turnoAtual === 'P') ? `${prefixoPadrao}P` : prefixoPadrao;
    let index = filaPacientes.findIndex(f => f.fila === siglaAlvo && f.status === 'LOBBY');
    
    if (index === -1) {
        const siglaAlternativa = (turnoAtual === 'P') ? prefixoPadrao : `${prefixoPadrao}P`;
        index = filaPacientes.findIndex(f => f.fila === siglaAlternativa && f.status === 'LOBBY');
    }

    if (index !== -1) {
        const ficha = filaPacientes[index];
        ficha.status = 'SALA';
        turnos[setorNome] = (turnos[setorNome] === 'P') ? 'N' : 'P';
        return ficha;
    }
    return null;
}

io.on('connection', (socket) => {
    socket.emit('atualizar_fila', filaPacientes);
    socket.emit('atualizar_painel_setores', ultimosChamados);
    enviarQuantitativosFila();
    socket.emit('estado_servidor', obterEstadoAtual());
    socket.emit('atualizar_media_setores', calcularMediaPorSetor());
    socket.emit('atualizar_estatisticas_absenteismo', calcularEstatisticasAbsenteismo());

    socket.on('estou_online', (nome) => {
        operadoresOnline[socket.id] = nome;
        io.emit('operadores_online_atualizados', Array.from(new Set(Object.values(operadoresOnline))));
    });

    socket.on('disconnect', () => {
        if(operadoresOnline[socket.id]) {
            delete operadoresOnline[socket.id];
            io.emit('operadores_online_atualizados', Array.from(new Set(Object.values(operadoresOnline))));
        }
    });

    socket.on('toggleFichaFisica', (idFicha) => {
        if (typeof idFicha !== 'string' || idFicha.length === 0 || idFicha.length > 20) return;
        if (fichasDesativadas.has(idFicha)) fichasDesativadas.delete(idFicha); 
        else fichasDesativadas.add(idFicha); 
    });

    socket.on('resetarLoteFisico', (prefixo) => {
        if (typeof prefixo !== 'string' || !Object.keys(contadores).includes(prefixo)) return;
        fichasDesativadas.forEach(ficha => {
            if (ficha.startsWith(`${prefixo}-`)) fichasDesativadas.delete(ficha);
        });
    });

    socket.on('pedir_dados_auditoria', () => {
        socket.emit('receber_dados_auditoria', historicoAtendimentos);
    });

    socket.on('pedir_minha_producao', (nomeOperador) => {
        const dataHoje = getDataString(); 
        const meusAtendimentos = historicoAtendimentos.filter(ficha => 
            ficha.atendente === nomeOperador && ficha.data === dataHoje && ficha.resultado === 'atendido'
        );
        socket.emit('receber_minha_producao', meusAtendimentos.length);
    });

    // === GERAÇÃO E IMPRESSÃO ===
    socket.on('adicionar_ficha', (dados) => {
        const prefixo = dados.filaOpcao;
        const prefixosValidos = Object.keys(contadores);
        if (!prefixosValidos.includes(prefixo)) {
            socket.emit('erro_sem_paciente_na_sala', 'Tipo de ficha inválido.');
            return;
        }

        // Emissão ilimitada: pula apenas números de fichas físicas desativadas,
        // sem nenhum teto máximo que force reinício da contagem.
        while (fichasDesativadas.has(`${prefixo}-${contadores[prefixo].toString().padStart(2, '0')}`)) {
            contadores[prefixo]++;
        }
        const numeroValidado = contadores[prefixo];
        contadores[prefixo]++;

        const numeroString = numeroValidado.toString().padStart(2, '0');
        const codigoFicha = `${prefixo} ${numeroString}`;

        const novaFicha = {
            id: Date.now().toString(),
            ficha: codigoFicha,
            nome: dados.nome ? String(dados.nome).trim().slice(0, 100) : '',
            setor: mapeamentoSetores[prefixo] || 'Regulação',
            fila: prefixo,
            status: 'LOBBY',
            horarioEmissao: new Date().toISOString(),
            horarioAtendimento: null
        };

        filaPacientes.push(novaFicha);
        io.emit('comando_imprimir_senha', novaFicha);
        io.emit('atualizar_fila', filaPacientes);
        enviarQuantitativosFila();
        emitirEstadoCompleto();
        salvarDados();
    });

    socket.on('chamar_para_atendimento', (setorDoPainel) => {
        if (tvFalando) return;
        let setor = setorDoPainel;
        let guiche = null;
        if (typeof setorDoPainel === 'object' && setorDoPainel !== null) {
            setor = setorDoPainel.setor;
            guiche = setorDoPainel.guiche || null;
        }

        const setoresValidos = ['REGULACAO', 'COMPLEXIDADE', 'AUTORIZACAO'];
        if (!setoresValidos.includes(setor)) return;
        if (guiche !== null && (typeof guiche !== 'string' && typeof guiche !== 'number')) guiche = null;
        if (typeof guiche === 'string') guiche = guiche.slice(0, 10);

        const nomeSetorReal = (setor === 'REGULACAO') ? 'Regulação' : (setor === 'COMPLEXIDADE') ? 'Complexidade' : 'Autorização';
        const prefixo = (setor === 'REGULACAO') ? 'R' : (setor === 'COMPLEXIDADE') ? 'C' : 'AT';
        let pacienteEscolhido;
        
        if (setor === 'AUTORIZACAO') {
            let idx = filaPacientes.findIndex(p => p.setor === 'Autorização' && p.status === 'LOBBY');
            if (idx !== -1) {
                pacienteEscolhido = filaPacientes[idx];
                pacienteEscolhido.status = 'SALA';
            }
        } else {
            pacienteEscolhido = extrairFichaComRegra(setor, prefixo);
        }

        if (pacienteEscolhido) {
            pacienteEscolhido.horarioAtendimento = new Date().toISOString();
            ultimosChamados[nomeSetorReal].unshift({ ficha: pacienteEscolhido.ficha, nome: pacienteEscolhido.nome });
            if (ultimosChamados[nomeSetorReal].length > 2) ultimosChamados[nomeSetorReal].pop();
            const pacoteDeChamada = { ficha: pacienteEscolhido.ficha, nome: pacienteEscolhido.nome, guiche };
            enfileirarChamadaTV(pacoteDeChamada);

            socket.emit('paciente_enviado_para_mesa', { paciente: pacienteEscolhido, guiche });
            io.emit('atualizar_painel_setores', ultimosChamados);
            io.emit('atualizar_fila', filaPacientes);
            enviarQuantitativosFila();
            emitirEstadoCompleto();
            salvarDados();
        } else {
            socket.emit('erro_sem_paciente_na_sala', 'Não há pacientes aguardando para o seu setor.');
        }
    });

    socket.on('rechamar_paciente_tv', (pacienteRechamado) => {
        const pacoteDeChamada = { ficha: pacienteRechamado.ficha, nome: pacienteRechamado.nome, guiche: pacienteRechamado.guiche };
        enfileirarChamadaTV(pacoteDeChamada);
    });

    socket.on('registrar_conclusao_atendimento', (dados) => {
        if (!dados || typeof dados !== 'object') return;
        const { setor, resultado, idFicha, operador, ubs, procedimentos } = dados;
        const resultadosValidos = ['atendido', 'falta'];
        if (!resultadosValidos.includes(resultado)) return;
        const atendente = (typeof operador === 'string' && operador.trim()) ? operador.trim().slice(0, 100) : 'Desconhecido';
        const ubsSegura = (typeof ubs === 'string' && ubs.trim()) ? ubs.trim().slice(0, 150) : 'Não informada';
        const procedimentosSeguro = (typeof procedimentos === 'string' && procedimentos.trim()) ? procedimentos.trim().slice(0, 300) : 'Nenhum';
        const idFichaSegura = (typeof idFicha === 'string' || typeof idFicha === 'number') ? idFicha : null;
        const siglaFichaSegura = typeof dados.siglaFicha === 'string' ? dados.siglaFicha : null;
        const idx = filaPacientes.findIndex(p => p.id === idFichaSegura || p.ficha === siglaFichaSegura);

        if (idx !== -1) {
            const paciente = filaPacientes[idx];
            const horaChegada = paciente.horarioEmissao || new Date(Number(paciente.id)).toISOString();
            const horaAtendimento = paciente.horarioAtendimento || new Date().toISOString();
            const tempoEspera = Math.max(0, Math.round((new Date(horaAtendimento) - new Date(horaChegada)) / 60000));

            const novoRegistro = {
                id: paciente.id, ficha: paciente.ficha, setor: paciente.setor, horaChegada, horaAtendimento,
                atendente, tempoEspera, resultado, ubs: ubsSegura, procedimentos: procedimentosSeguro,
                data: getDataString() 
            };
            historicoAtendimentos.push(novoRegistro);
            // Envia só o registro novo pros clientes (ex: ubs.html, resumo.html),
            // ao invés do histórico inteiro — que já passa de 1400 registros.
            io.emit('novo_registro_auditoria', novoRegistro);
            // Grava esse registro como documento próprio no Mongo (não reenvia o
            // histórico inteiro a cada atendimento — ver salvarDados()).
            if (dbCofre) {
                dbCofre.collection('historico_atendimentos').insertOne(novoRegistro)
                    .catch(err => console.error("❌ Falha ao gravar histórico no Mongo:", err));
            }

            filaPacientes.splice(idx, 1);
            if (resultado === 'atendido') atendimentosPorOperador[atendente] = (atendimentosPorOperador[atendente] || 0) + 1;
            if (resultado === 'falta' && setor !== 'AUTORIZACAO') {
                const isPriority = paciente.fila.endsWith('P');
                turnos[setor] = isPriority ? 'P' : 'N';
            }
            io.emit('atualizar_fila', filaPacientes);
            enviarQuantitativosFila();
            emitirEstadoCompleto();
            salvarDados();
        }
        socket.emit('guiche_liberado_com_sucesso');
    });

    socket.on('tv_terminou_de_falar', () => {
        clearTimeout(timerSegurancaTV); 
        liberarServidorEProximo();
    });

    socket.on('excluir_ficha', (idFicha) => {
        filaPacientes = filaPacientes.filter(p => p.id !== idFicha);
        io.emit('atualizar_fila', filaPacientes);
        enviarQuantitativosFila();
        emitirEstadoCompleto();
        salvarDados();
    });

    socket.on('resetar_sistema', () => {
        realizarResetGeral();
    });
});

async function iniciarServidor() {
    // Agora ele carrega da nuvem primeiro, se falhar, vai pro disco local!
    await carregarDados(); 
    agendarResetMeiaNoite();
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => console.log(`🚀 Motor Híbrido rodando na porta ${PORT}`));
}

iniciarServidor();