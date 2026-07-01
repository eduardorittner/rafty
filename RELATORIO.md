# Relatório Técnico: Implementação do Protocolo Raft em Rust com Target WASM

## 1. Introdução do Problema

### 1.1 Contextualização sobre Algoritmos de Consenso Distribuído

Em sistemas distribuídos, o problema do consenso consiste em fazer com que múltiplos processos (ou nós) concordem sobre um único valor ou sequência de valores, mesmo na presença de falhas parciais do sistema. Este problema é fundamental para a construção de sistemas tolerantes a falhas, como bancos de dados distribuídos, sistemas de coordenação e blockchains.

### 1.2 Algoritmos Anteriores ao Raft

O algoritmo **Paxos**, proposto por Leslie Lamport em 1989, estabeleceu-se como o primeiro algoritmo de consenso prático e formalmente verificado. Apesar de sua correção matemática, o Paxos tornou-se notório por sua complexidade de compreensão e implementação. A descrição original do algoritmo utiliza uma abordagem baseada em "propostas" e "aceites" que, embora elegante do ponto de vista teórico, resulta em código difícil de depurar e manter na prática.

Outras variações do Paxos, como **Multi-Paxos** (para consenso sobre sequências de valores) e **Fast Paxos** (para otimização de latência), herdaram esta complexidade, limitando sua adoção em sistemas práticos. A dificuldade de implementação correta levou muitos engenheiros a desenvolverem soluções ad-hoc, frequentemente incorretas ou incompletas.

### 1.3 O Diferencial do Raft

Publicado em 2014 por Diego Ongaro e John Ousterhout, o **Raft** foi projetado com um objetivo explícito: ser **compreensível**. Diferentemente do Paxos, que foi desenvolvido primeiramente como um exercício teórico, o Raft foi concebido desde o início como uma base prática para implementação de sistemas reais.

As principais inovações do Raft em termos de compreensibilidade incluem:

1. **Decomposição modular**: O algoritmo separa claramente as sub-tarefas de consenso em componentes distintos: eleição de líder, replicação de log e segurança.

2. **Fortes garantias de estado**: O Raft impõe restrições mais fortes que o Paxos, limitando o espaço de estados possíveis e simplificando o raciocínio sobre o sistema.

3. **Mudança de configuração segura**: O Raft introduz um mecanismo baseado em consenso para adicionar ou remover nós do cluster de forma segura, sem interromper o serviço.

4. **Terminologia intuitiva**: Conceitos como "líder", "seguidor" e "candidato" tornam o algoritmo mais acessível a desenvolvedores.

---

## 2. Algoritmo Raft

### 2.1 Visão Geral de Alto Nível

O Raft gerencia um log replicado que contém uma sequência de comandos a serem executados pela máquina de estados de cada nó do cluster. O algoritmo garante que, mesmo na presença de falhas de nós (desde que a maioria permaneça operacional), todos os nós corretos concordem sobre a mesma sequência de comandos.

### 2.2 Estados dos Nós

Cada nó no Raft encontra-se em exatamente um dos três estados:

- **Seguidor (Follower)**: Estado passivo onde o nó responde a requisições de líderes e candidatos. Seguidores não iniciam requisições RPC por conta própria.

- **Candidato (Candidate)**: Estado transitório iniciado quando um seguidor não recebe comunicação do líder dentro de um período determinado. Candidatos solicitam votos de outros nós para tentar se tornar líderes.

- **Líder (Leader)**: Estado ativo responsável por gerenciar toda a replicação de log. Em um cluster estável, há exatamente um líder. Todas as requisições de clientes são processadas pelo líder.

### 2.3 Termos e Eleições

O tempo no Raft é dividido em **termos**, que são números inteiros sequenciais que atuam como relógios lógicos. Cada termo começa com uma eleição:

1. Quando um seguidor não recebe mensagens do líder dentro de um **timeout de eleição** (valor aleatório entre limites configurados), ele transita para candidato.

2. O candidato incrementa seu termo, vota em si mesmo e envia requisições de voto (**RequestVote RPC**) para todos os outros nós.

3. Um nó concede seu voto se:
   - O termo do candidato é maior ou igual ao termo atual
   - O candidato possui um log pelo menos tão completo quanto o seu (comparação de último índice e termo)
   - Ainda não votou em outro candidato no termo atual

4. Um candidato torna-se líder ao receber votos da **maioria** dos nós do cluster.

5. Se múltiplos candidatos emergirem simultaneamente, nenhum receberá maioria, resultando em **empate**. Neste caso, novos timeouts de eleição (aleatórios) garantem que eventualmente um único candidato vença.

### 2.4 Replicação de Log

Uma vez eleito, o líder é responsável por replicar entradas de log para os seguidores:

1. O líder recebe comandos de clientes e os adiciona ao seu log local.

2. O líder envia **AppendEntries RPC** para todos os seguidores, contendo as novas entradas.

3. Cada seguidor valida a consistência do log comparando o índice e termo da entrada anterior. Se consistente, a entrada é adicionada ao log local.

4. Quando a maioria dos nós replicou uma entrada, ela é considerada **comitada** e o líder notifica os seguidores para aplicá-la às suas máquinas de estado.

### 2.5 Heartbeats

Para manter sua autoridade, o líder envia periodicamente **heartbeats** (AppendEntries RPCs vazias) para todos os seguidores. Se um seguidor não recebe heartbeat dentro do timeout de eleição, ele assume que o líder falhou e inicia nova eleição.

### 2.6 Segurança do Raft

O Raft garante várias propriedades de segurança críticas:

- **Eleição com log completo**: Um candidato só pode ser eleito se seu log estiver pelo menos tão completo quanto o de qualquer outro nó.

- **Líder sempre possui entradas comitadas**: Devido à restrição de eleição, um líder sempre possui todas as entradas comitadas em seus logs.

- **Casamento de log (Log Matching)**: Se dois logs possuem uma entrada com mesmo índice e termo, então todos os logs até aquele índice são idênticos.

---

## 3. Detalhes da Implementação

### 3.1 Arquitetura Geral

A implementação analisada, denominada **Rafty**, é desenvolvida em Rust com suporte a WebAssembly (WASM), permitindo execução tanto em ambientes nativos quanto em navegadores. O projeto está estruturado em múltiplos crates:

```
rafty/
├── src/
│   ├── proto/          # Definições e serialização protobuf
│   └── raft/           # Core do protocolo Raft
└── harness/            # Framework de testes e simulação
```

### 3.2 Core como Máquina de Estados Push/Pull

O núcleo da implementação é estruturado como uma **máquina de estados finita** que processa mensagens de forma determinística. A estrutura central é a struct `Node<Store, Chan, Rng>`, parametrizada por três tipos genéricos que permitem injeção de dependências para teste:

```rust
pub struct Node<Store: Storage, Chan: Channel, Rng: RngProvider> {
    pub id: ValidNodeId,
    pub term: u64,
    pub voted_for: NodeId,
    pub leader_id: NodeId,
    pub role: Role,
    pub config: InitialConfig,
    pub storage: RaftLog<Store>,
    pub channel: Chan,
    pub rng: Rng,
    pub election_timeout: u64,
}
```

#### 3.2.1 Método `step()` - Processamento de Mensagens

O método `step()` implementa a lógica de transição de estados baseada em mensagens recebidas:

```rust
pub fn step(&mut self, msg: Message) -> Result<()> {
    match msg {
        Message::StartCampaign => self.start_campaign(),
        Message::Heartbeat(m) => self.step_heartbeat(m),
        Message::HeartbeatResponse(m) => self.step_heartbeat_response(m),
        Message::Append(m) => self.step_append(m)?,
        Message::AppendResponse(m) => self.step_append_response(m)?,
        Message::RequestVote(m) => self.step_vote_request(m),
        Message::RequestVoteResponse(m) => self.step_vote_response(m),
    }
    Ok(())
}
```

Cada variante de mensagem dispara um manipulador específico que atualiza o estado interno do nó conforme as regras do protocolo Raft.

#### 3.2.2 Método `tick()` - Avanço Temporal

O método `tick()` simula a passagem do tempo, incrementando contadores internos e disparando ações baseadas em timeout:

```rust
pub fn tick(&mut self) {
    match &mut self.role {
        Role::Follower(state) => {
            state.ticks_since_last_msg += 1;
            if state.election_timeout_passed(self.election_timeout) {
                let _ = self.step(Message::StartCampaign);
            }
        }
        Role::Candidate(state) => {
            state.ticks_since_election_start += 1;
            if state.votes.has_majority_for() {
                // Transição para líder
            }
        }
        Role::Leader(state) => {
            state.ticks_since_last_heartbeat += 1;
            if state.ticks_since_last_heartbeat >= self.config.ticks_between_heartbeats {
                self.broadcast_heartbeats();
            }
        }
    }
}
```

Esta separação entre processamento de eventos (`step`) e avanço temporal (`tick`) caracteriza o padrão **push/pull**: mensagens são "empurradas" para o nó via `step()`, enquanto o nó "puxa" informações temporais via `tick()`.

### 3.3 Driver com Interface Amigável

O módulo `harness` fornece uma camada de abstração que envolve o core Raft com interfaces mais acessíveis para testes e integração:

#### 3.3.1 Cluster

A struct `Cluster<Rng>` gerencia múltiplos nós Raft em um único processo, facilitando testes de integração:

```rust
pub struct Cluster<Rng: RngProvider = raft::DefaultRng> {
    pub nodes: Vec<TestNode<Rng>>,
    pub paused_nodes: HashSet<u64>,
    pub message_buffer: Vec<ClusterMessage>,
    state_callbacks: Vec<Box<dyn FnMut(&ClusterEvent)>>,
    pub tick_rate_ms: u64,
    pub rng: Rng,
}
```

Operações como `tick()`, `step()`, `pause_node()` e `resume_node()` permitem simular cenários complexos de falha de forma controlada.

#### 3.3.2 Suporte WASM

O projeto inclui configuração específica para compilação WASM através do script `wasm.sh`:

```bash
cd harness && wasm-pack build --release --target web --out-dir pkg --out-name rafty_wasm
```

Módulos condicionais (`#[cfg(target_arch = "wasm32")]`) fornecem implementações específicas para ambiente web, incluindo `WasmCluster` e tipos serializáveis para comunicação com JavaScript.

### 3.4 Sistema de Comunicação com Protobuf

A serialização e desserialização de mensagens é realizada utilizando **Protocol Buffers (protobuf)**, um mecanismo eficiente e language-agnostic desenvolvido pelo Google.

#### 3.4.1 Definição das Mensagens

O arquivo `message.proto` define a estrutura das mensagens:

```protobuf
syntax = "proto3";
package proto.message;

message Entry {
  uint64 term = 1;
  uint64 index = 2;
  bytes data = 3;
}

message ProtoMessage {
  ProtoMessageType msg_type = 1;
  uint64 to = 2;
  uint64 from = 3;
  uint64 term = 4;
  uint64 commit = 5;
  uint64 last_term = 6;
  uint64 last_index = 7;
  repeated Entry entries = 8;
  uint64 voted_for = 9;
  bool success = 10;
}

enum ProtoMessageType {
  Heartbeat = 0;
  HeartbeatResponse = 1;
  AppendEntries = 2;
  AppendEntriesResponse = 3;
  RequestVote = 4;
  RequestVoteResponse = 5;
}
```

#### 3.4.2 Camada de Tipo Seguro

Para melhorar a segurança de tipos em Rust, a implementação introduz um enum `Message` que encapsula as variantes específicas com seus campos relevantes:

```rust
pub enum Message {
    Heartbeat(Heartbeat),
    HeartbeatResponse(Heartbeat),
    Append(Append),
    AppendResponse(AppendResponse),
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    StartCampaign,
}
```

Conversões bidirecionais (`From<Message> for ProtoMessage` e vice-versa) permitem transição transparente entre a representação tipada interna e a representação serializável externa.

#### 3.4.3 Implementação TCP

Para comunicação em ambiente nativo, a struct `TcpChannel` implementa o trait `Channel` utilizando sockets TCP:

```rust
impl Channel for TcpChannel {
    fn send(&mut self, msg: ProtoMessage) {
        let bytes = msg.encode_to_vec();
        // Envia via TCP stream
    }
    
    fn broadcast(&mut self, msg: ProtoMessage) {
        let bytes = msg.encode_to_vec();
        // Broadcast para todos os canais
    }
}
```

---

## 4. Como Foi Testado

### 4.1 Estratégia de Testes

A implementação do Raft foi validada através de uma abordagem de testes em múltiplas camadas, combinando testes unitários determinísticos com testes de simulação estocástica. O framework de testes foi projetado para ser **reprodutível** e **injetável**, permitindo a simulação controlada de falhas.

### 4.2 Testes de Simulação Determinística

Os testes determinísticos são executados no módulo `harness/tests/` e validam comportamentos específicos do protocolo de forma reproduzível. Cada teste foca em um aspecto particular do algoritmo:

#### 4.2.1 Testes de Eleição (`election.rs`)

- **`start_campaign`**: Verifica que um seguidor transita para candidato após o timeout de eleição.
- **`elect_leader`**: Valida que um candidato torna-se líder ao receber maioria de votos.
- **`elect_leader_right_after_majority`**: Confirma que a transição para líder ocorre imediatamente após obtenção da maioria.
- **`leader_not_elected_with_one_vote`**: Demonstra que um candidato não pode ser eleito com apenas seu próprio voto.
- **`election_after_leader_fails`**: Simula falha do líder e verifica que novos candidatos emergem.
- **`stale_leader_steps_down`**: Valida que um líder com termo inferior cede autoridade a um candidato com termo superior.
- **`election_candidate_log_less_up_to_date`**: Verifica que candidatos com logs menos completos não recebem votos.
- **`election_split_vote_resolution`**: Demonstra resolução de empate em eleições divididas.

#### 4.2.2 Testes de Replicação (`entry_replication.rs`, `append_entries.rs`)

Validam a correta replicação de entradas de log do líder para seguidores, incluindo tratamento de inconsistências e retransmissão.

#### 4.2.3 Testes de Sanidade (`sanity_check.rs`)

Verificam invariantes básicos do sistema, como unicidade de líder por termo e consistência de termos entre nós.

### 4.3 Testes Randomizados com Fault Injection

O módulo `randomized.rs` implementa testes estocásticos que utilizam geradores de números aleatórios determinísticos para garantir reprodutibilidade:

```rust
fn run_randomized_test<F>(test_fn: F)
where
    F: FnOnce(u64) + std::panic::UnwindSafe,
{
    let seed = env::var("SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    
    // Executa teste com seed específica
    // Em caso de falha, imprime seed para reprodução
}
```

#### 4.3.1 Tipos de Testes Randomizados

1. **`randomized_lossy_network`**: Simula rede com perda de pacotes entre 10% e 40%, validando que um líder é eventualmente eleito e mantido.

2. **`randomized_transient_node_outages`**: Simula falhas transitórias de nós, onde nós são aleatoriamente pausados e retomados, garantindo que pelo menos uma maioria permaneça ativa.

3. **`randomized_log_agreement`**: Após eleição de líder e replicação de entradas, verifica que todos os nós possuem logs consistentes até o índice comitado mínimo.

4. **`election_fails_with_total_network_partition`**: Valida que, com 100% de perda de mensagens, nenhuma eleição pode ser completada (propriedade de segurança).

### 4.4 Interfaces que Facilitam Testes

A arquitetura da implementação foi deliberadamente projetada para facilitar testes através de **injeção de dependência** via traits.

#### 4.4.1 Trait `Storage`

A trait `Storage` abstrai o armazenamento persistente do log Raft:

```rust
pub trait Storage {
    fn last_index(&self) -> u64;
    fn term(&self, idx: u64) -> Result<u64>;
    fn last_term(&self) -> u64;
    fn entries(&self, low: u64, high: u64) -> Result<Vec<Entry>>;
    fn append(&mut self, entries: Vec<Entry>) -> Result<()>;
}
```

A implementação **`MemStorage`** utilizada nos testes é um armazenamento volátil baseado em `Vec`, que não persiste entre crashes do processo:

```rust
pub struct MemStorage {
    log: Vec<Entry>,
}
```

Esta simplificação permite testes rápidos e isolados, sem a complexidade de I/O de disco ou recuperação de falhas.

#### 4.4.2 Trait `Channel`

A trait `Channel` abstrai a comunicação entre nós:

```rust
pub trait Channel {
    fn send(&mut self, msg: ProtoMessage);
    fn broadcast(&mut self, msg: ProtoMessage);
}
```

A implementação de teste **`TestChannel<Rng>`** utiliza canais `mpsc` (multi-producer, single-consumer) da biblioteca padrão para comunicação intra-processo:

```rust
pub struct TestChannel<Rng: RngProvider> {
    pub channels: Vec<FaultyChannel<Rng>>,
    pub recv: Receiver<ProtoMessage>,
    pub id: u64,
    pub on_message_sent: Option<MessageCallback>,
}
```

#### 4.4.3 Injeção de Falhas via `FaultyChannel`

O `FaultyChannel` envolve um canal de envio com uma taxa de descarte configurável:

```rust
pub struct FaultyChannel<Rng: RngProvider> {
    pub channel: Sender<ProtoMessage>,
    pub drop_rate: FaultRate,
    pub rng: Rng,
}

impl<Rng: RngProvider> FaultyChannel<Rng> {
    pub fn send(&mut self, msg: ProtoMessage) {
        if self.rng.random_range(1, 101) <= self.drop_rate.0 as u64 {
            self.channel.send(msg).expect("Write to test channel failed");
        }
        // Caso contrário, mensagem é descartada (simula perda de rede)
    }
}
```

Constantes como `NO_FAULT` (100% de entrega) e `ONLY_FAULT` (0% de entrega) facilitam a configuração de cenários extremos.

#### 4.4.4 Trait `RngProvider`

A trait `RngProvider` abstrai a geração de números aleatórios:

```rust
pub trait RngProvider: Send + Sync + Clone + std::fmt::Debug + 'static {
    fn random_range(&mut self, low: u64, high: u64) -> u64;
}
```

Duas implementações são fornecidas:

- **`DefaultRng`**: Utiliza o gerador aleatório do sistema para produção.
- **`DeterministicRng`**: Utiliza `StdRng` com seed fixa para testes reproduzíveis.

Esta abstração permite que testes randomizados sejam executados com seeds específicas, garantindo que falhas possam ser reproduzidas deterministicamente.

### 4.5 Cobertura de Cenários de Falha

A combinação das interfaces testáveis permite simular os seguintes cenários de falha:

| Tipo de Falha | Mecanismo de Simulação |
|--------------|------------------------|
| Perda de pacotes | `FaultyChannel` com `drop_rate` configurável |
| Falha de nó | `Cluster::pause_node()` / `resume_node()` |
| Partição de rede | `ONLY_FAULT` entre subconjuntos de nós |
| Atraso de mensagens | (Implementável via `FaultyChannel` com delay) |
| Falha de líder | Pausar nó líder e aguardar nova eleição |
| Logs inconsistentes | Manipulação direta de `MemStorage` antes do teste |

---

## 5. Conclusão

A implementação do protocolo Raft apresentada neste relatório demonstra uma abordagem moderna e bem estruturada para consenso distribuído. A separação clara entre o core do protocolo (máquina de estados push/pull) e as camadas de abstração (driver, comunicação, armazenamento) segue princípios de design que favorecem testabilidade e manutenibilidade.

O uso de traits genéricas para `Storage`, `Channel` e `RngProvider` permite que a mesma implementação core seja testada em diversos cenários de falha sem modificação do código principal. Esta arquitetura facilita não apenas testes unitários e de integração, mas também a extensão do sistema para diferentes backends de armazenamento e protocolos de comunicação.

A inclusão de suporte a WebAssembly amplia o escopo de aplicação da implementação, permitindo sua utilização em ambientes de navegador para fins educacionais, de visualização ou até mesmo em aplicações distribuídas que operam parcialmente no client-side.

Os testes implementados, combinando abordagens determinísticas e randomizadas, fornecem cobertura abrangente dos comportamentos esperados do protocolo Raft, incluindo cenários adversos de rede e falhas de nós. A capacidade de reproduzir testes falhos através de seeds específicas é particularmente valiosa para depuração de condições de corrida e outros bugs concorrentes.

---

## Referências

1. ONGARO, D.; OUSTERHOUT, J. In Search of an Understandable Consensus Algorithm. **USENIX Annual Technical Conference**, 2014.

2. LAMPORT, L. The Part-Time Parliament. **ACM Transactions on Computer Systems**, v. 16, n. 2, p. 133-169, 1998.

3. Google Protocol Buffers Documentation. Disponível em: <https://developers.google.com/protocol-buffers>.

4. The Rust Programming Language Documentation. Disponível em: <https://doc.rust-lang.org/>.

5. WebAssembly Documentation. Disponível em: <https://webassembly.org/>.
