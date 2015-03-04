package com.relateiq.fdq.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.relateiq.fdq.Consumer;
import com.relateiq.fdq.Helpers;
import com.relateiq.fdq.MessageRequest;
import com.relateiq.fdq.Producer;
import com.relateiq.fdq.TopicManager;
import com.relateiq.fdq.TopicProducer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Created by mbessler on 2/23/15.
 */
public class FDQCli {
    public static final Logger log = LoggerFactory.getLogger(FDQCli.class);
    private static final Random random = new Random();

    enum Command {
        PRODUCE, PRODUCE_RANDOM, CONSUME, STATUS, DEACTIVATE, ACTIVATE, NUKE
    };

    public static void main(String[] args) throws JsonProcessingException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("fdq");
//        parser.addArgument("--foo").action(Arguments.storeTrue()).help("foo help");
        Subparsers subparsers = parser.addSubparsers().help("sub-command help");

        addTopicCommand(subparsers, "produce", Command.PRODUCE, "read tuples from STDIN, in the format of: [shardKey] [message]");
        addTopicCommand(subparsers, "random", Command.PRODUCE_RANDOM, "insert many random tuples into a topic");
        addTopicCommand(subparsers, "consume", Command.CONSUME, "consume a topic, outputting tuples to STDOUT");
        addTopicCommand(subparsers, "status", Command.STATUS, "return status info about a topic in JSON");
        addTopicCommand(subparsers, "deactivate", Command.DEACTIVATE, "deactivate a topic's conusmers");
        addTopicCommand(subparsers, "activate", Command.ACTIVATE, "activate a topic's consumers");
        addTopicCommand(subparsers, "nuke", Command.NUKE, "erase a topic, all of its data, and all of its configuration entirely");

        Namespace parsed = null;
        try {
            parsed = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }


//        Subparser parserB = subparsers.addParser("b").help("b help");
//        parserB.addArgument("--baz").choices("X", "Y", "Z").help("baz help");
        String topic = parsed.getString("topic");

        switch ((Command)parsed.get("command")) {
            case PRODUCE:
                produce(topic);
                break;
            case PRODUCE_RANDOM:
                produceRandom(topic);
                break;
            case CONSUME:
                consume(topic);
                break;
            case STATUS:
                status(topic);
                break;
            case DEACTIVATE:
                deactivate(topic);
                break;
            case ACTIVATE:
                activate(topic);
                break;
            case NUKE:
                nuke(topic);
                break;
        }
    }

    private static void addTopicCommand(Subparsers subparsers, String commandName, Command command, String help) {
        Subparser produceParser = subparsers.addParser(commandName).help(help)
                .setDefault("command", command);
        produceParser.addArgument("topic").type(String.class).help("the name of the topic");
    }

    private static void activate(String topic) {

    }

    private static void deactivate(String topic) {

    }

    private static void nuke(String topic) {
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();
        TopicManager manager = new TopicManager(db, Helpers.createTopicConfig(db, topic));

        manager.nuke();
    }

    private static void status(String topic) throws JsonProcessingException {
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();
        TopicManager manager = new TopicManager(db, Helpers.createTopicConfig(db, topic));
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        System.out.println(mapper.writeValueAsString(manager.stats()));
    }

    private static void produceRandom(String topic) {
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        TopicProducer p = new Producer(db).createProducer(topic);

        while (true) {
            List<MessageRequest> reqs = IntStream.range(0, 1000).mapToObj(i -> new MessageRequest("" + i, ("qwerty " + i).getBytes())).collect(toList());
            p.produceBatch(reqs);

//            p.produce(Long.toString(random.nextLong()), Long.toString(random.nextLong()).getBytes());
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                break;
//            }

        }
    }

    private static void consume(String topic) {
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Consumer c = new Consumer(db);

        c.consume(topic, "" + (new Random()).nextLong(), e -> {
            try {
                Thread.sleep(new Random().nextInt(6000));
            } catch (InterruptedException e1) {
            }
            System.out.println(e.toString() + " " + new String(e.message));});

    }

    private static void produce(String topic) {
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        TopicProducer p = new Producer(db).createProducer(topic);

        try {
            BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
            for(String s = null; (s = b.readLine()) != null;){
                String[] parts = s.split(" ", 2);
                if (parts.length > 1) {
                    String shardKey = parts[0];
                    String message = parts[1];
                    p.produce(shardKey, message.getBytes());
                } else {
                    p.produce(null, s.getBytes());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
