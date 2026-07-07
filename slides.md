---
marp: true
theme: gaia
paginate: true
backgroundColor: #0b0f19
color: #e2e8f0
style: |
  @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');
  
  section {
    font-family: 'Plus Jakarta Sans', sans-serif;
    background: radial-gradient(circle at 10% 20%, rgb(15, 23, 42) 0%, rgb(9, 13, 26) 90.1%);
    color: #cbd5e1;
    padding: 50px 80px;
    font-size: 24px;
    line-height: 1.5;
  }
  
  h1 {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-weight: 800;
    color: #f8fafc;
    font-size: 44px;
    margin-bottom: 20px;
    background: linear-gradient(135deg, #38bdf8 0%, #818cf8 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }

  h2 {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-weight: 700;
    color: #38bdf8;
    font-size: 30px;
    margin-top: 0;
  }

  h3 {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-weight: 600;
    color: #94a3b8;
    font-size: 22px;
    margin-bottom: 8px;
    margin-top: 0;
  }

  p, li {
    font-weight: 400;
  }

  strong {
    color: #38bdf8;
    font-weight: 600;
  }

  code {
    background-color: #1e293b;
    color: #f1f5f9;
    border-radius: 6px;
    padding: 2px 6px;
    font-family: 'Courier New', Courier, monospace;
    font-size: 0.85em;
  }

  pre {
    background-color: #0f172a;
    border: 1px solid #334155;
    border-radius: 8px;
    padding: 15px;
  }

  footer {
    font-size: 14px;
    color: #64748b;
    position: absolute;
    bottom: 20px;
    left: 80px;
  }

  .lead {
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    text-align: center;
  }

  .lead h1 {
    font-size: 50px;
    line-height: 1.2;
    background: linear-gradient(135deg, #60a5fa 0%, #c084fc 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 20px;
  }

  .lead h3 {
    margin-top: 10px;
    color: #94a3b8;
    font-weight: 400;
  }

  .grid-2 {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 30px;
  }

  .card {
    background: rgba(30, 41, 59, 0.4);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 12px;
    padding: 20px;
    margin-top: 10px;
  }

  .card h3 {
    margin-top: 0;
    color: #38bdf8;
  }

  ul {
    margin-top: 10px;
    margin-bottom: 10px;
    padding-left: 20px;
  }

  li {
    margin-bottom: 6px;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 20px;
    font-size: 18px;
  }

  th {
    background-color: #1e293b;
    color: #38bdf8;
    text-align: left;
    padding: 10px;
    border-bottom: 2px solid #334155;
  }

  td {
    padding: 10px;
    border-bottom: 1px solid #1e293b;
  }

  tr:hover {
    background-color: rgba(56, 189, 248, 0.05);
  }
---

<!-- _class: lead -->

# O Protocolo de Consenso Raft
### Consenso Distribuído de Alta Compreensibilidade

**Sistemas Distribuídos** • Visão Geral do Algoritmo

Eduardo Rittner Coelho - RA 250960

---

# O Problema do Consenso
Como fazer nós independentes entrarem em acordo sobre uma sequência de valores na presença de falhas?

<div class="grid-2">
<div class="card">
<h3>Por que é difícil?</h3>
<ul>
  <li>Falhas de hardware (crashes de nós)</li>
  <li>Perda ou duplicação de mensagens</li>
  <li>Partições de rede temporárias e latência</li>
</ul>
</div>
<div class="card">
<h3>Aplicações Críticas</h3>
<ul>
  <li>Bancos de dados distribuídos consistentes</li>
  <li>Sistemas de coordenação (ex: ZooKeeper)</li>
  <li>Tecnologias de Ledger/Blockchain</li>
</ul>
</div>
</div>

---

# Máquinas de Estados Replicadas
A abordagem central para construir serviços distribuídos tolerantes a falhas.

<div class="grid-2">
<div class="card">
  <h3>Teoria Básica</h3>
  <ul>
    <li>Múltiplos nós executam <b>máquinas de estado determinísticas</b> independentes.</li>
    <li>Se todos os nós processarem a mesma sequência de entradas na mesma ordem, chegarão ao mesmo estado final.</li>
  </ul>
</div>
<div class="card">
  <h3>O Papel do Log</h3>
  <ul>
    <li>Cada entrada no log é um <b>comando</b> a ser executado localmente por cada máquina de estados independente.</li>
    <li>O consenso garante que todos os nós concordem exatamente com a <b>ordem global</b> dos comandos.</li>
  </ul>
</div>
</div>

---

# Evolução dos Algoritmos de Consenso

<div class="grid-2">
<div>
  <h3>Viewstamped Replication (1988)</h3>
  <p>Desenvolvido por Oki e Liskov. Pioneiro no uso de nós primários (líderes) e visões temporais ("views").</p>
  <h3>Paxos (1989)</h3>
  <p>Proposto por Leslie Lamport. Primeiro algoritmo prático formalmente verificado, porém com complexidade notória de entendimento e implementação.</p>
</div>
<div>
  <h3>Multi-Paxos & Fast Paxos</h3>
  <p>Variações desenvolvidas para otimizar latência e sequenciamento de valores, herdando e agravando a complexidade teórica do Paxos original.</p>
</div>
</div>

---

# Raft: Foco na Compreensibilidade
Proposto em 2014 por Ongaro e Ousterhout como uma alternativa prática e compreensível ao Paxos.

* **Decomposição Modular:** Separa explicitamente a eleição de líder, replicação de log e garantia de segurança.
* **Fortes Garantias de Estado:** Restringe o espaço de estados válidos para simplificar o raciocínio matemático.
* **Consenso Simplificado:** Lógica de maioria simples para commits (quorum) sem as múltiplas fases complexas do Paxos.
* **Mudança Dinâmica de Configuração:** Transição de composição do cluster de forma segura.

---

# Os Três Estados do Raft
Um nó do cluster opera estritamente em um dos três papéis em qualquer instante:

<div class="grid-2">
<div class="card">
  <h3>1. Seguidor (Follower)</h3>
  <p>Estado passivo. Apenas responde a requisições RPC de candidatos e líderes. Se sofrer timeout de eleição, transita para candidato.</p>
</div>
<div class="card">
  <h3>2. Candidato (Candidate)</h3>
  <p>Estado temporário para eleição. Incrementa o termo lógico, vota em si mesmo e envia requisições de voto (RequestVote RPC).</p>
</div>
</div>

<div class="card" style="margin-top:20px;">
  <h3>3. Líder (Leader)</h3>
  <p>Gerencia as escritas dos clientes, coordena a replicação de logs e envia heartbeats para manter autoridade.</p>
</div>

---

# Ciclo de Eleição de Líder
Como o cluster elege um novo coordenador de forma resiliente:

* **Regra de Voto Único:** Um nó só pode conceder o seu voto a **um único candidato por eleição/termo**.
* **Timeouts de Eleição:** Cada seguidor espera um intervalo aleatório para evitar colisões de votos (*split votes*).
* **Processo de Votos:**
  * Candidatos solicitam votos via `RequestVote RPC`.
  * Seguidores votam se o log do candidato for pelo menos tão atualizado e o termo for maior ou igual ao atual.
* **Resultado:**
  * **Vitória:** Exige maioria absoluta dos votos do cluster.
  * **Sem Vencedor:** A eleição pode terminar sem nenhum vencedor (empate), iniciando um novo termo/eleição.

---

# Replicação de Log (Log Replication)
O mecanismo para sincronizar a máquina de estados distribuída.

1. **Proposta:** Líder recebe comandos do cliente e adiciona ao log local.
2. **Disseminação:** Líder envia o comando aos seguidores via `AppendEntries RPC`.
3. **Consistência:** Seguidores validam índice e termo anteriores para garantir integridade histórica (**Log Matching**).
4. **Commit:** Uma entrada é comitada e executada quando a maioria dos nós a replica com sucesso.
