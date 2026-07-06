# Lab 2 - MC714 - 250960

## Rafty: Protocolo Raft em Rust

O relatório final pode ser encontrado em https://eduardorittner.github.io/rafty/.

---

## 1. Pré-requisitos

Para rodar todos os cenários do projeto, você precisará ter as seguintes ferramentas instaladas em sua máquina:

### 1.1 Rust e Cargo
Instale a ferramenta de gerenciamento do Rust seguindo o [Guia Oficial de Instalação do Rust](https://www.rust-lang.org/tools/install) ou executando:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
Adicione o target WebAssembly necessário para compilação WASM:
```bash
rustup target add wasm32-unknown-unknown
```

### 1.2 Compilador Protocol Buffers (`protoc`)
Necessário para a compilação das mensagens RPC (`message.proto`) usando a crate `prost-build`.
* Siga o [Guia de Instalação do protoc](https://grpc.io/docs/protoc-installation/) ou instale via gerenciador de pacotes:
  * **macOS** (Homebrew): `brew install protobuf`
  * **Ubuntu/Debian**: `sudo apt install -y protobuf-compiler`
  * **Windows** (via `scoop` ou `choco`): `scoop install protobuf` ou `choco install protoc`

### 1.3 `wasm-pack`
Ferramenta para compilar, empacotar e integrar código Rust com WebAssembly.
* Consulte o [Guia de Instalação do wasm-pack](https://rustwasm.github.io/wasm-pack/installer/) ou instale com:
```bash
cargo install wasm-pack
```

### 1.4 Node.js e NPM
Necessários para rodar o servidor de desenvolvimento da interface visual (Vite).
* Baixe e instale a versão LTS a partir da [Página Oficial do Node.js](https://nodejs.org/).

### 1.5 Docker e Docker Compose
Necessários para rodar o cenário multi-nós em rede simulada próxima da produção.
* Baixe e instale seguindo o [Guia Oficial do Docker Desktop](https://docs.docker.com/get-docker/).

---

## 🚀 2. Compilando e Testando o Núcleo (Rust)

Para compilar todo o workspace e garantir que todos os testes unitários e de integração (eleição, replicação de entradas, comportamento caótico de rede) estão passando:

```bash
# Compilar o projeto
cargo build

# Rodar todos os testes unitários e de integração
cargo test
```

---

## 🐳 3. Cenário Multi-Nós com Docker Compose

Este cenário simula um cluster de 3 nós Raft reais se comunicando via sockets TCP. O gerenciamento é feito via o arquivo `Makefile` na raiz do projeto.

### Inicializando o cluster
```bash
# Buildar e iniciar os containers em segundo plano (background)
make up
```

Após iniciar, o cluster estará rodando com 3 nós mapeados nas seguintes portas locais:
* **Nó 1**: Dashboard em `http://localhost:8081`
* **Nó 2**: Dashboard em `http://localhost:8082`
* **Nó 3**: Dashboard em `http://localhost:8083`

### Interagindo com o Cluster
Cada nó expõe um servidor HTTP simples contendo os seguintes endpoints úteis:

1. **Dashboard Visual**: Abra `http://localhost:8081` no seu navegador para ver o estado do nó, logs internos e os logs replicados em tempo real.
2. **Consultar Estado (JSON)**:
   ```bash
   curl http://localhost:8081/status
   ```
3. **Enviar Proposta de Escrita**:
   ```bash
   curl -X POST -d '{"data": "sua_mensagem_aqui"}' http://localhost:8081/propose
   ```
   *Se o nó para o qual você enviou a escrita for o Líder, ele propagará e confirmará a escrita. Se for um Seguidor, a escrita será rejeitada (indicando a necessidade de enviar ao líder).*

### Encerrando o cluster
```bash
# Para parar os containers
make down

# Para limpar os containers, volumes e imagens locais geradas
make clean
```

---

## 🌐 4. Visualizador Web interativo (WebAssembly + Vite)

Este cenário compila a lógica do Raft para WebAssembly e a executa diretamente no navegador, fornecendo uma visualização dinâmica de eventos do cluster.

### Passo 1: Compilar o código Rust para WASM
Execute o script utilitário na raiz do projeto:
```bash
# Torne o script executável (caso necessário)
chmod +x wasm.sh

# Execute a compilação
./wasm.sh
```
*(Este script executa: `cd harness && wasm-pack build --release --target web --out-dir pkg --out-name rafty_wasm`)*

### Passo 2: Executar o servidor Web do Vite
```bash
# Acesse o diretório web
cd web

# Instale as dependências de desenvolvimento do Node.js
npm install

# Inicie o servidor local de desenvolvimento
npm run dev
```

### Passo 3: Acessar a interface
Abra no navegador o link indicado pelo terminal (geralmente `http://localhost:5173`). 

Na interface, você poderá:
* Visualizar o cluster rodando em um diagrama interativo.
* Pausar/retomar nós individuais para testar o comportamento do cluster em falhas.
* Alterar a velocidade do tempo de simulação (*tick rate*).
* Forçar novas eleições e ver a passagem de mensagens de eleição/heartbeat de forma visual.
