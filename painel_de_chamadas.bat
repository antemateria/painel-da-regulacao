@echo off
title Servidor Painel de Senhas
echo Verificando dependencias...
call npm install
echo Iniciando o sistema de chamadas...
node server.js
pause
