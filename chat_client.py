#!/usr/bin/env python3

import sys
import threading
import re
import json

from kafka import KafkaProducer, KafkaConsumer


def subscriptions(consumer):
    subs = consumer.subscription()
    return list(subs) if subs is not None else []


def add_subscription(consumer, topic):
    topics = set(subscriptions(consumer))
    topics.add(topic)
    consumer.subscribe(list(topics))


# Supprime un abonnement à un topic
def del_subscription(consumer, topic):
    topics = set(subscriptions(consumer))
    if topic not in topics:
        raise RuntimeError(f"Le topic {topic} n'est pas dans les abonnements.")
    topics.remove(topic)
    if topics:
        consumer.subscribe(list(topics))
    else:
        consumer.unsubscribe()


should_quit = False


def read_messages(consumer):
    # TODO À compléter
    while not should_quit:
        # On utilise poll pour ne pas bloquer indéfiniment quand should_quit
        # devient True
        received = consumer.poll(100)

        for channel, messages in received.items():
            for msg in messages:
                try:
                    data = json.loads(msg.value.decode())
                    print(f"{data['nick']}: {data['msg']}")
                except Exception:
                    print("< %s: %s" % (channel.topic, msg.value))



def cmd_msg(producer, channel, line, nick, consumer):
    if channel is None:
        print("Aucun canal actif. Utilisez /join pour en rejoindre un.")
        return

    topic = "chat_channel_" + channel[1:]

    # Vérifie si le canal est réellement rejoint
    if topic not in subscriptions(consumer):
        print("Vous n’êtes pas abonné à ce canal.")
        return

    if not line:
        return

    payload = json.dumps({"nick": nick, "msg": line})
    try:
        producer.send(topic, payload.encode())
    except Exception as e:
        print("Erreur lors de l'envoi du message :", e)


def is_valid_channel(chan):
    return re.match(r"^#[a-zA-Z0-9-]+$", chan)


def cmd_join(consumer, producer, line, nick):
    if not line or not is_valid_channel(line):
        print("Nom de canal invalide. Ex: #general")
        return False

    topic = "chat_channel_" + line[1:]
    try:
        add_subscription(consumer, topic)
        message = f"{nick} has joined"
        producer.send(topic, message.encode())
        return True
    except Exception as e:
        print("Erreur lors du join :", e)
        return False


def cmd_part(consumer, producer, line, nick):
    if not line or not is_valid_channel(line):
        print("Nom de canal invalide. Ex : #general")
        return False

    topic = "chat_channel_" + line[1:]

    if topic not in subscriptions(consumer):
        print("Vous n’êtes pas sur ce canal.")
        return False

    try:
        del_subscription(consumer, topic)
        producer.send(topic, f"{nick} has left".encode())
        return True
    except Exception as e:
        print("Erreur lors du part :", e)
        return False



def cmd_quit(producer, line):
    # TODO À compléter
    pass



def main_loop(nick, consumer, producer):
    joined_chans = []
    curchan = None

    while True:
        try:
            if curchan is None:
                line = input("> ")
            else:
                line = input("[%s]> " % curchan)
        except EOFError:
            print("/quit")
            line = "/quit"

        if line.startswith("/"):
            cmd, *args = line[1:].split(" ", maxsplit=1)
            cmd = cmd.lower()
            args = None if args == [] else args[0]
        else:
            cmd = "msg"
            args = line

        if cmd == "msg":
            cmd_msg(producer, curchan, args, nick, consumer)
        elif cmd == "join":
            success = cmd_join(consumer, producer, args, nick)
            if success:
                joined_chans.append(args)
                curchan = args

        elif cmd == "part":
            success = cmd_part(consumer, producer, args, nick)
            if success:
                if args in joined_chans:
                    joined_chans.remove(args)
                if curchan == args:
                    curchan = joined_chans[-1] if joined_chans else None

        elif cmd == "quit":
            cmd_quit(producer, args)
            break
        # TODO: rajouter des commandes ici



def main():
    if len(sys.argv) != 2:
        print("usage: %s nick" % sys.argv[0])
        return 1

    nick = sys.argv[1]
    consumer = KafkaConsumer()
    producer = KafkaProducer()

    th = threading.Thread(target=read_messages, args=(consumer,))
    th.start()

    try:
        main_loop(nick, consumer, producer)
    finally:
        global should_quit
        should_quit = True
        th.join()



if __name__ == "__main__":
    sys.exit(main())
