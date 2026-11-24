import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Nodo ATM distribuito basato su algoritmo di mutua esclusione a Token Ring.
 *
 * <p>Ogni istanza gira in un processo JVM indipendente senza memoria condivisa:
 * lo stato del conto bancario viaggia esclusivamente all'interno del messaggio
 * Token trasmesso lungo l'anello logico via socket TCP su localhost (127.0.0.1).
 *
 * <p>Topologia fissa:
 * ATM1 (5001) -> ATM2 (5002) -> ATM3 (5003) -> ATM4 (5004) -> ATM1 (5001)
 *
 * <p>Avvio:
 * {@code java ATMNode <1..4> [--starter] [saldo_iniziale]}
 */
public class ATMNode {

    private static final String HOST = "127.0.0.1";
    private static final int TOTAL_NODES = 4;
    private static final int BASE_PORT = 5000;
    private static final BigDecimal DEFAULT_INITIAL_BALANCE = new BigDecimal("1000.00");

    // Ritardo controllato (in ms) prima di inoltrare il token al nodo successivo.
    // Rende i log leggibili in tempo reale e consente all'utente di inserire comandi CLI.
    private static final long TOKEN_HOLD_DELAY_MS = 800L;

    // Intervallo e timeout per l'invio TCP (supporta l'avvio asincrono dei 4 terminali).
    private static final int SOCKET_CONNECT_TIMEOUT_MS = 2000;
    private static final int SOCKET_READ_TIMEOUT_MS = 5000;
    private static final long RETRY_DELAY_MS = 1000L;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final int nodeId;
    private final String nodeName;
    private final int listenPort;
    private final int nextNodeId;
    private final int nextPort;
    private final boolean isStarter;
    private final BigDecimal initialBalance;

    // Coda locale thread-safe delle transazioni richieste dall'utente tramite CLI.
    // Il thread CLI accoda richieste mentre il thread Worker le consuma in sezione critica.
    private final Queue<Transaction> pendingTransactions = new ConcurrentLinkedQueue<>();

    // Coda di handoff a capacità 1 tra il Listener TCP e il Worker che esegue la sezione critica.
    // Garantisce che in ogni istante il nodo gestisca al più un token attivo.
    private final BlockingQueue<TokenMessage> tokenInbox = new ArrayBlockingQueue<>(1);

    private volatile boolean running = true;
    private volatile ServerSocket serverSocket;
    private Thread listenerThread;
    private Thread workerThread;

    /**
     * Rappresenta il messaggio di Token che circola sull'anello TCP.
     * Formato serializzato su rete: {@code TOKEN|<id>|<saldo>|<seq>}
     */
    private static final class TokenMessage {
        final String senderId;
        final BigDecimal balance;
        final long sequence;

        TokenMessage(String senderId, BigDecimal balance, long sequence) {
            this.senderId = senderId;
            this.balance = balance.setScale(2, RoundingMode.HALF_UP);
            this.sequence = sequence;
        }

        /**
         * Serializza il token nel formato testuale del protocollo: {@code TOKEN|id|saldo|seq}.
         */
        String serialize() {
            return String.format(
                    Locale.US,
                    "TOKEN|%s|%s|%d",
                    senderId,
                    balance.toPlainString(),
                    sequence
            );
        }

        /**
         * Deserializza e valida una riga ricevuta dal socket TCP.
         *
         * @throws IllegalArgumentException se il messaggio non rispetta il formato atteso
         */
        static TokenMessage parse(String rawLine) {
            if (rawLine == null || rawLine.isBlank()) {
                throw new IllegalArgumentException("Messaggio token vuoto");
            }
            String[] parts = rawLine.trim().split("\\|");
            if (parts.length != 4 || !"TOKEN".equals(parts[0])) {
                throw new IllegalArgumentException("Formato token non valido: " + rawLine);
            }
            String senderId = parts[1].trim();
            if (senderId.isEmpty()) {
                throw new IllegalArgumentException("ID mittente mancante nel token: " + rawLine);
            }
            BigDecimal balance;
            long seq;
            try {
                balance = new BigDecimal(parts[2].trim()).setScale(2, RoundingMode.HALF_UP);
                seq = Long.parseLong(parts[3].trim());
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Valori numerici non validi nel token: " + rawLine, ex);
            }
            return new TokenMessage(senderId, balance, seq);
        }
    }

    private enum TxType {
        DEPOSITO,
        PRELIEVO
    }

    /**
     * Rappresenta una singola operazione bancaria accodata dall'utente locale.
     */
    private static final class Transaction {
        final TxType type;
        final BigDecimal amount;

        Transaction(TxType type, BigDecimal amount) {
            this.type = type;
            this.amount = amount.setScale(2, RoundingMode.HALF_UP);
        }

        @Override
        public String toString() {
            return type.name() + " " + amount.toPlainString() + " EUR";
        }
    }

    public ATMNode(int nodeId, boolean isStarter, BigDecimal initialBalance) {
        this.nodeId = nodeId;
        this.nodeName = "ATM" + nodeId;
        this.listenPort = BASE_PORT + nodeId;
        this.nextNodeId = (nodeId % TOTAL_NODES) + 1;
        this.nextPort = BASE_PORT + nextNodeId;
        this.isStarter = isStarter;
        this.initialBalance = initialBalance.setScale(2, RoundingMode.HALF_UP);
    }

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 3) {
            printUsageAndExit("Numero di argomenti non valido.");
        }

        int parsedNodeId = -1;
        boolean parsedStarter = false;
        BigDecimal parsedBalance = null;

        try {
            parsedNodeId = Integer.parseInt(args[0]);
            if (parsedNodeId < 1 || parsedNodeId > TOTAL_NODES) {
                printUsageAndExit("L'ID del nodo deve essere compreso tra 1 e 4.");
            }
        } catch (NumberFormatException ex) {
            printUsageAndExit("ID nodo non valido: " + args[0]);
        }

        for (int i = 1; i < args.length; i++) {
            String arg = args[i].trim();
            if ("--starter".equalsIgnoreCase(arg)) {
                if (parsedStarter) {
                    printUsageAndExit("Flag --starter specificato più volte.");
                }
                parsedStarter = true;
            } else if (parsedBalance == null) {
                parsedBalance = parsePositiveOrZeroAmount(arg);
                if (parsedBalance == null) {
                    printUsageAndExit("Saldo iniziale non valido (deve essere >= 0): " + arg);
                }
            } else {
                printUsageAndExit("Argomento imprevisto: " + arg);
            }
        }

        if (parsedBalance == null) {
            parsedBalance = DEFAULT_INITIAL_BALANCE;
        }

        ATMNode node = new ATMNode(parsedNodeId, parsedStarter, parsedBalance);
        node.start();
    }

    private static void printUsageAndExit(String errorMessage) {
        System.err.println("Errore: " + errorMessage);
        System.err.println("Uso: java ATMNode <1..4> [--starter] [saldo_iniziale]");
        System.exit(1);
    }

    /**
     * Avvia il server TCP locale, il worker di gestione del token e la CLI interattiva.
     */
    public void start() {
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName(HOST), listenPort));
        } catch (IOException ex) {
            System.err.printf("Impossibile avviare %s su %s:%d -> %s%n",
                    nodeName, HOST, listenPort, ex.getMessage());
            System.exit(1);
        }

        printBanner();

        // Thread dedicato esclusivamente all'ascolto delle connessioni TCP dal predecessore.
        listenerThread = new Thread(this::listenForTokenLoop, nodeName + "-TCP-Listener");
        listenerThread.start();

        // Thread dedicato al possesso del token, esecuzione della sezione critica e inoltro.
        workerThread = new Thread(this::tokenWorkerLoop, nodeName + "-Token-Worker");
        workerThread.start();

        // =========================================================================
        // GESTIONE DEL TOKEN INIZIALE (NODO --starter)
        // =========================================================================
        // Solo il nodo avviato con --starter genera il Token iniziale e lo immette
        // nella propria coda locale (tokenInbox). Da quel momento, il token inizierà
        // a circolare nell'anello portando con sé il saldo iniziale e il numero di sequenza 1.
        if (isStarter) {
            TokenMessage genesisToken = new TokenMessage(nodeName, initialBalance, 1L);
            log("INFO", String.format("Nodo starter attivo: generato token iniziale %s",
                    genesisToken.serialize()));
            tokenInbox.offer(genesisToken);
        }

        // Il thread principale gestisce l'interfaccia a riga di comando (CLI) dell'utente.
        runCliLoop();
    }

    private void printBanner() {
        log("INFO", String.format(
                "Avviato %s in ascolto su %s:%d -> nodo successivo ATM%d (%s:%d)%s",
                nodeName,
                HOST,
                listenPort,
                nextNodeId,
                HOST,
                nextPort,
                isStarter ? " [STARTER, saldo iniziale=" + initialBalance.toPlainString() + " EUR]" : ""
        ));
        synchronized (System.out) {
            System.out.println("Comandi disponibili:");
            System.out.println("  d <importo>  -> Accoda un deposito (es. d 150.00)");
            System.out.println("  p <importo>  -> Accoda un prelievo (es. p 200.00)");
            System.out.println("  demo         -> Carica le transazioni di esempio per questo nodo");
            System.out.println("  exit         -> Termina il nodo ATM");
            System.out.println();
        }
    }

    /**
     * =========================================================================
     * SCAMBIO DI MESSAGGI TCP (RICEZIONE DAL NODO PREDECESSORE)
     * =========================================================================
     * Rimane in ascolto sul {@link ServerSocket} locale. Per ogni connessione in
     * ingresso legge esattamente una riga UTF-8 contenente {@code TOKEN|id|saldo|seq},
     * valida il messaggio, registra {@code RICEZIONE_TOKEN} e consegna il token al
     * thread Worker tramite {@code tokenInbox}.
     *
     * <p>La separazione tra ricezione socket e worker permette di chiudere subito
     * la connessione TCP col mittente senza trattenerla aperta durante il tempo di
     * elaborazione della sezione critica.
     */
    private void listenForTokenLoop() {
        while (running) {
            try (Socket clientSocket = serverSocket.accept()) {
                clientSocket.setSoTimeout(SOCKET_READ_TIMEOUT_MS);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
                String line = reader.readLine();
                if (line == null) {
                    continue;
                }

                TokenMessage receivedToken = TokenMessage.parse(line);
                log("RICEZIONE_TOKEN", String.format(
                        "Ricevuto %s (da=%s, saldo=%s EUR, seq=%d)",
                        receivedToken.serialize(),
                        receivedToken.senderId,
                        receivedToken.balance.toPlainString(),
                        receivedToken.sequence
                ));

                // Consegna il token al worker per l'ingresso nella sezione critica.
                // put() assicura handoff bloccante nel caso improbabile di token già in coda.
                tokenInbox.put(receivedToken);
            } catch (SocketException ex) {
                // SocketException durante la chiusura di serverSocket in fase di shutdown è attesa.
                if (running) {
                    log("ERRORE", "Errore socket in ricezione: " + ex.getMessage());
                }
            } catch (IllegalArgumentException ex) {
                log("ERRORE", "Messaggio TCP scartato: " + ex.getMessage());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException ex) {
                if (running) {
                    log("ERRORE", "Errore I/O durante la ricezione del token: " + ex.getMessage());
                }
            }
        }
    }

    /**
     * =========================================================================
     * GESTIONE DEL TOKEN E LOGICA DELLA SEZIONE CRITICA
     * =========================================================================
     * Ciclo principale di possesso del token:
     * 1. Attende di ricevere il token in {@code tokenInbox}.
     * 2. Finché detiene il token, il nodo è l'UNICO nell'anello autorizzato a
     *    leggere e modificare il saldo condiviso (mutua esclusione distribuita).
     * 3. Se ci sono transazioni locali in {@code pendingTransactions}, ne esegue
     *    una per giro in ordine FIFO (politica fair che evita starvation degli altri ATM).
     * 4. Applica una breve pausa di circolazione per rendere osservabili i log.
     * 5. Aggiorna {@code senderId} e incrementa {@code sequence}, registra
     *    {@code INOLTRO_TOKEN} e invia il nuovo token al nodo successivo via TCP.
     */
    private void tokenWorkerLoop() {
        while (running) {
            TokenMessage currentToken;
            try {
                currentToken = tokenInbox.take();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }

            if (!running) {
                break;
            }

            // --- INGRESSO NELLA SEZIONE CRITICA ---
            BigDecimal updatedBalance = executeCriticalSectionIfNeeded(currentToken.balance);

            // Breve attesa prima di rilasciare il token lungo l'anello, così i terminali
            // restano leggibili e l'utente ha il tempo di interagire con la CLI.
            if (running) {
                try {
                    Thread.sleep(TOKEN_HOLD_DELAY_MS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    if (!running) {
                        break;
                    }
                }
            }

            if (!running) {
                break;
            }

            // --- USCITA DALLA SEZIONE CRITICA E INOLTRO DEL TOKEN ---
            // Prepara il nuovo token con l'identificativo di questo nodo come ultimo
            // detentore, il saldo eventualmente aggiornato e il numero di sequenza +1.
            TokenMessage nextToken = new TokenMessage(
                    nodeName,
                    updatedBalance,
                    currentToken.sequence + 1L
            );

            forwardTokenWithRetry(nextToken);
        }
    }

    /**
     * Esegue la sezione critica sul saldo contenuto nel token.
     * Estrae al massimo una transazione dalla coda locale FIFO per garantire equità
     * tra tutti i nodi dell'anello.
     *
     * @param currentBalance saldo corrente trasportato dal token
     * @return saldo risultante al termine dell'eventuale transazione
     */
    private BigDecimal executeCriticalSectionIfNeeded(BigDecimal currentBalance) {
        Transaction tx = pendingTransactions.poll();
        if (tx == null) {
            return currentBalance;
        }

        log("INIZIO_TX", String.format("Esecuzione %s su saldo corrente %s EUR",
                tx, currentBalance.toPlainString()));

        BigDecimal newBalance = currentBalance;
        if (tx.type == TxType.DEPOSITO) {
            newBalance = currentBalance.add(tx.amount);
            log("SALDO_AGGIORNATO", String.format(
                    "Deposito +%s EUR completato | Saldo precedente: %s EUR -> Nuovo saldo: %s EUR",
                    tx.amount.toPlainString(),
                    currentBalance.toPlainString(),
                    newBalance.toPlainString()
            ));
        } else if (tx.type == TxType.PRELIEVO) {
            // Controllo di consistenza monetaria nella sezione critica:
            // impedisce saldi negativi quando più ATM prelevano in concorrenza logica.
            if (currentBalance.compareTo(tx.amount) >= 0) {
                newBalance = currentBalance.subtract(tx.amount);
                log("SALDO_AGGIORNATO", String.format(
                        "Prelievo -%s EUR completato | Saldo precedente: %s EUR -> Nuovo saldo: %s EUR",
                        tx.amount.toPlainString(),
                        currentBalance.toPlainString(),
                        newBalance.toPlainString()
                ));
            } else {
                log("SALDO_AGGIORNATO", String.format(
                        "Prelievo -%s EUR RIFIUTATO per fondi insufficienti | Saldo invariato: %s EUR",
                        tx.amount.toPlainString(),
                        currentBalance.toPlainString()
                ));
            }
        }

        log("FINE_TX", String.format("Transazione %s terminata (saldo nel token=%s EUR)",
                tx.type.name(), newBalance.toPlainString()));

        return newBalance;
    }

    /**
     * =========================================================================
     * SCAMBIO DI MESSAGGI TCP (INOLTRO AL NODO SUCCESSIVO CON RETRY)
     * =========================================================================
     * Tenta di aprire una connessione TCP verso {@code HOST:nextPort} e di inviare
     * la stringa serializzata del token terminata da newline.
     *
     * <p>Poiché i 4 processi vengono avviati manualmente in terminali separati, il
     * successore potrebbe non essere ancora pronto al primo invio (es. quando il nodo
     * {@code --starter} viene avviato per primo). Il ciclo di retry con pausa evita
     * la perdita del token e consente l'allineamento automatico dell'anello.
     */
    private void forwardTokenWithRetry(TokenMessage token) {
        log("INOLTRO_TOKEN", String.format(
                "Invio %s verso ATM%d (%s:%d)",
                token.serialize(),
                nextNodeId,
                HOST,
                nextPort
        ));

        boolean waitingLogged = false;
        while (running) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(HOST, nextPort), SOCKET_CONNECT_TIMEOUT_MS);
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                writer.write(token.serialize());
                writer.write('\n');
                writer.flush();

                if (waitingLogged) {
                    log("INFO", String.format("Connessione ristabilita con ATM%d (%s:%d): token consegnato.",
                            nextNodeId, HOST, nextPort));
                }
                return;
            } catch (IOException ex) {
                if (!running) {
                    return;
                }
                if (!waitingLogged) {
                    log("ATTESA_ANELLO", String.format(
                            "ATM%d (%s:%d) non ancora raggiungibile (%s). Riprovo ogni %d ms...",
                            nextNodeId, HOST, nextPort, ex.getMessage(), RETRY_DELAY_MS
                    ));
                    waitingLogged = true;
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * Loop interattivo da terminale per l'inserimento dei comandi utente.
     */
    private void runCliLoop() {
        try (Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8)) {
            while (running && scanner.hasNextLine()) {
                String rawInput = scanner.nextLine().trim();
                if (rawInput.isEmpty()) {
                    continue;
                }

                String[] tokens = rawInput.split("\\s+");
                String command = tokens[0].toLowerCase(Locale.ROOT);

                switch (command) {
                    case "d" -> handleDepositOrWithdraw(TxType.DEPOSITO, tokens);
                    case "p" -> handleDepositOrWithdraw(TxType.PRELIEVO, tokens);
                    case "demo" -> handleDemoCommand();
                    case "exit" -> {
                        shutdown();
                        return;
                    }
                    default -> log("CLI", "Comando sconosciuto. Usa: d <importo>, p <importo>, demo, exit");
                }
            }
        } catch (IllegalStateException ex) {
            // Ignorato durante la chiusura ordinata dello stream.
        }
        if (running) {
            shutdown();
        }
    }

    private void handleDepositOrWithdraw(TxType type, String[] tokens) {
        if (tokens.length != 2) {
            log("CLI", String.format("Uso corretto: %s <importo>", type == TxType.DEPOSITO ? "d" : "p"));
            return;
        }
        BigDecimal amount = parseStrictlyPositiveAmount(tokens[1]);
        if (amount == null) {
            log("CLI", "Importo non valido: deve essere un numero maggiore di 0.");
            return;
        }
        Transaction tx = new Transaction(type, amount);
        pendingTransactions.offer(tx);
        log("CLI", String.format("Transazione accodata in attesa del token: %s", tx));
    }

    /**
     * Carica le transazioni predefinite dello scenario dimostrativo:
     * ATM2 -> prelievo 200.00 EUR
     * ATM3 -> deposito 100.00 EUR
     * ATM4 -> prelievo 500.00 EUR
     */
    private void handleDemoCommand() {
        switch (nodeId) {
            case 2 -> {
                Transaction tx = new Transaction(TxType.PRELIEVO, new BigDecimal("200.00"));
                pendingTransactions.offer(tx);
                log("CLI", "Demo caricata su ATM2: accodato " + tx);
            }
            case 3 -> {
                Transaction tx = new Transaction(TxType.DEPOSITO, new BigDecimal("100.00"));
                pendingTransactions.offer(tx);
                log("CLI", "Demo caricata su ATM3: accodato " + tx);
            }
            case 4 -> {
                Transaction tx = new Transaction(TxType.PRELIEVO, new BigDecimal("500.00"));
                pendingTransactions.offer(tx);
                log("CLI", "Demo caricata su ATM4: accodato " + tx);
            }
            default -> log("CLI",
                    "Scenario demo: nessuna transazione su ATM1. Digita 'demo' su ATM2 (p 200), ATM3 (d 100) e ATM4 (p 500).");
        }
    }

    /**
     * Arresta il nodo ATM liberando il socket e interrompendo i thread interni.
     */
    private void shutdown() {
        if (!running) {
            return;
        }
        running = false;
        log("INFO", "Arresto di " + nodeName + " in corso...");

        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
                // Chiusura best-effort in fase di uscita.
            }
        }
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
        if (workerThread != null) {
            workerThread.interrupt();
        }
        log("INFO", nodeName + " terminato.");
    }

    private static BigDecimal parsePositiveOrZeroAmount(String raw) {
        return parseDecimal(raw, true);
    }

    private static BigDecimal parseStrictlyPositiveAmount(String raw) {
        return parseDecimal(raw, false);
    }

    private static BigDecimal parseDecimal(String raw, boolean allowZero) {
        try {
            String normalized = raw.trim().replace(',', '.');
            BigDecimal value = new BigDecimal(normalized).setScale(2, RoundingMode.HALF_UP);
            int cmp = value.compareTo(BigDecimal.ZERO);
            if (allowZero ? cmp < 0 : cmp <= 0) {
                return null;
            }
            return value;
        } catch (NumberFormatException | ArithmeticException ex) {
            return null;
        }
    }

    /**
     * Stampa una riga di log sincronizzata con timestamp preciso al millisecondo.
     */
    private void log(String event, String message) {
        String ts = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
        synchronized (System.out) {
            System.out.printf("[%s] [%s] [%-16s] %s%n", ts, nodeName, event, message);
        }
    }
}
