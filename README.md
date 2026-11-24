# Sistema Distribuito ATM con Mutua Esclusione a Token Ring

Progetto Java stand-alone (JDK 17+, nessuna dipendenza esterna) che implementa un sistema distribuito di transazioni bancarie su 4 nodi ATM indipendenti, sincronizzati tramite algoritmo di mutua esclusione a **Token Ring** su socket TCP.

---

## Architettura del Sistema

Ogni nodo ATM viene eseguito in un **processo JVM separato** su `127.0.0.1` senza alcuna memoria condivisa. L'unico meccanismo di coordinamento e di trasporto dello stato del conto bancario è il passaggio del **Token** lungo l'anello unidirezionale:

```text
ATM1 (127.0.0.1:5001) ───▶ ATM2 (127.0.0.1:5002)
         ▲                          │
         │                          ▼
ATM4 (127.0.0.1:5004) ◀─── ATM3 (127.0.0.1:5003)
```

### Principi Chiave

1. **Mutua Esclusione Distribuita**: soltanto il nodo che detiene il Token in un dato istante può accedere alla **sezione critica** ed eseguire una transazione sul saldo.
2. **Stato nel Messaggio Token**: il saldo bancario viaggia all'interno del messaggio scambiato via TCP con formato:
   ```text
   TOKEN|<id>|<saldo>|<seq>
   ```
   - `id`: identificativo dell'ultimo nodo ATM che ha inoltrato il token (es. `ATM1`).
   - `saldo`: importo corrente con precisione decimale `BigDecimal` a 2 cifre (default iniziale `1000.00` EUR).
   - `seq`: numero di sequenza monotono incrementato ad ogni passaggio nell'anello.
3. **Fairness (Equità)**: al ricevimento del Token, ogni nodo estrae ed esegue al più una transazione per giro dalla propria coda locale FIFO (`ConcurrentLinkedQueue`), evitando la starvation degli altri terminali.
4. **Concorrenza Locale**:
   - `TCP Listener Thread`: accetta connessioni su `ServerSocket`, valida il messaggio `TOKEN|id|saldo|seq`, registra `RICEZIONE_TOKEN` e passa il token al worker.
   - `Token Worker Thread`: gestisce l'eventuale sezione critica, registra `INIZIO_TX`, `SALDO_AGGIORNATO`, `FINE_TX`, applica una pausa controllata (`800 ms`) per la leggibilità dei log e inoltra il token (`INOLTRO_TOKEN`) al successore con retry automatico durante l'avvio dei terminali.
   - `CLI Thread (main)`: legge in modo interattivo i comandi dell'utente senza bloccare la circolazione del Token.

---

## Requisiti

- **Java Development Kit (JDK) 17** o superiore (`javac` e `java` nel `PATH`).
- Nessuna libreria o build tool esterno richiesto.

---

## Compilazione

Aprire un terminale nella cartella del progetto ed eseguire:

```bash
javac ATMNode.java
```

---

## Avvio dei 4 Terminali

Aprire **4 finestre di terminale distinte** nella stessa cartella del progetto ed eseguire un comando per ciascun terminale.

> [!NOTE]
> L'ordine di avvio è flessibile grazie al retry TCP automatico del nodo `--starter`, ma avviare prima i nodi `2`, `3`, `4` e infine il nodo `1` con `--starter` permette la circolazione immediata del Token.

**Terminale 2 (ATM2 — porta 5002):**
```bash
java ATMNode 2
```

**Terminale 3 (ATM3 — porta 5003):**
```bash
java ATMNode 3
```

**Terminale 4 (ATM4 — porta 5004):**
```bash
java ATMNode 4
```

**Terminale 1 (ATM1 — porta 5001, nodo starter con saldo default 1000.00 EUR):**
```bash
java ATMNode 1 --starter
```

*(Opzionale)* È possibile specificare un saldo iniziale personalizzato in fase di avvio del nodo `--starter`:
```bash
java ATMNode 1 --starter 2500.00
```

### Sintassi Completa

```bash
java ATMNode <1..4> [--starter] [saldo_iniziale]
```

---

## Comandi CLI Interattivi

In ciascuno dei 4 terminali è possibile digitare in qualsiasi momento:

| Comando | Descrizione | Esempio |
| :--- | :--- | :--- |
| `d <importo>` | Accoda un **deposito** sull'ATM locale | `d 150.50` |
| `p <importo>` | Accoda un **prelievo** sull'ATM locale (rifiutato se fondi insufficienti) | `p 200` |
| `demo` | Carica la transazione di esempio assegnata al nodo corrente (`ATM2`: prelievo `200`, `ATM3`: deposito `100`, `ATM4`: prelievo `500`) | `demo` |
| `exit` | Arresta in modo ordinato il processo ATM corrente | `exit` |

---

## Esecuzione dello Scenario `demo`

Con tutti e 4 i terminali avviati (saldo iniziale `1000.00 EUR`):

1. Nel terminale **ATM2**, digitare `demo` (accoda `PRELIEVO 200.00 EUR`).
2. Nel terminale **ATM3**, digitare `demo` (accoda `DEPOSITO 100.00 EUR`).
3. Nel terminale **ATM4**, digitare `demo` (accoda `PRELIEVO 500.00 EUR`).

Al passaggio del Token lungo l'anello:
- **ATM2** esegue `-200.00` → saldo aggiornato: `800.00 EUR`
- **ATM3** esegue `+100.00` → saldo aggiornato: `900.00 EUR`
- **ATM4** esegue `-500.00` → saldo aggiornato: `400.00 EUR`

### Esempio di Log al Millisecondo

```text
[2026-11-24 22:36:10.112] [ATM2] [RICEZIONE_TOKEN ] Ricevuto TOKEN|ATM1|1000.00|1 (da=ATM1, saldo=1000.00 EUR, seq=1)
[2026-11-24 22:36:10.113] [ATM2] [INIZIO_TX       ] Esecuzione PRELIEVO 200.00 EUR su saldo corrente 1000.00 EUR
[2026-11-24 22:36:10.114] [ATM2] [SALDO_AGGIORNATO] Prelievo -200.00 EUR completato | Saldo precedente: 1000.00 EUR -> Nuovo saldo: 800.00 EUR
[2026-11-24 22:36:10.114] [ATM2] [FINE_TX         ] Transazione PRELIEVO terminata (saldo nel token=800.00 EUR)
[2026-11-24 22:36:10.915] [ATM2] [INOLTRO_TOKEN   ] Invio TOKEN|ATM2|800.00|2 verso ATM3 (127.0.0.1:5003)
```
