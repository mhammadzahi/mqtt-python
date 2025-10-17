import paho.mqtt.client as mqtt
import time
import ssl

# --- Configuration ---
BROKER_ADDRESS = "test.mosquitto.org"
PORT = 1883
# For TLS, use port 8883
# PORT_TLS = 8883
TOPIC = "test 10"
CLIENT_ID = "client-123"

# --- Advanced: Last Will and Testament (LWT) ---
# This message is published by the broker if the client disconnects ungracefully.
LWT_TOPIC = "gemini/test/status"
LWT_MESSAGE = f"Client '{CLIENT_ID}' has disconnected unexpectedly."

# --- Callbacks ---
# These functions are called when specific events occur.

def on_connect(client, userdata, flags, rc):
    """Callback for when the client connects to the broker."""
    if rc == 0:
        print("Connected to MQTT Broker!")
        # Subscribe to the topic once connected. QoS 1 means the message is
        # guaranteed to be delivered at least once.
        client.subscribe(TOPIC, qos=1)
        print(f"Subscribed to topic: {TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}\n")

def on_message(client, userdata, msg):
    """Callback for when a PUBLISH message is received from the broker."""
    print(f"Received message: '{msg.payload.decode()}' on topic '{msg.topic}' | userdata: {userdata}")

def on_disconnect(client, userdata, rc):
    """Callback for when the client disconnects from the broker."""
    print(f"Disconnected with result code {rc}")
    if rc != 0:
        print("Unexpected disconnection.")

# --- Main Client Logic ---
def run_client():
    # Create a new MQTT client instance
    client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)

    # --- Advanced: Assign callbacks ---
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # --- Advanced: Set the Last Will and Testament ---
    # The broker will publish this message if the client loses connection without
    # sending a formal DISCONNECT packet.
    # retain=True means the LWT message will be kept by the broker for new subscribers.
    client.will_set(LWT_TOPIC, payload=LWT_MESSAGE, qos=2, retain=True)

    # --- Advanced: TLS/SSL Security (optional) ---
    # Uncomment the following lines to enable a secure connection.
    # You may need to provide paths to your CA certificate, client cert, and key.
    # client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
    #                tls_version=ssl.PROTOCOL_TLS, ciphers=None)
    # client.connect(BROKER_ADDRESS, PORT_TLS)

    # Connect to the broker
    try:
        client.connect(BROKER_ADDRESS, PORT, 60)
    except Exception as e:
        print(f"Error connecting to broker: {e}")
        return

    # client.loop_forever() is a blocking call that processes network traffic,
    # dispatches callbacks, and handles reconnecting.
    # It's suitable for scripts that do nothing else but listen for MQTT messages.
    client.loop_start()  # Use loop_start() for non-blocking background processing

    # Let's publish some messages
    try:
        count = 0
        while True:
            message = f"Hello from Python! Message #{count}"
            # Publish with QoS 1 to ensure it's delivered at least once.
            # retain=False means the message is not stored by the broker.
            result = client.publish(TOPIC, message, qos=1, retain=False)
            status = result.rc
            if status == 0:
                print(f"Sent `{message}` to topic `{TOPIC}`")
            else:
                print(f"Failed to send message to topic {TOPIC}")
            count += 1
            time.sleep(5)
    except KeyboardInterrupt:
        print("Disconnecting gracefully...")
        client.disconnect()
        client.loop_stop()

if __name__ == '__main__':
    run_client()
