import asyncio
import websockets
import json
import time

try:
    import RPi.GPIO as GPIO
    SIMULATION = False
except ImportError:
    SIMULATION = True
    print("[SIMULATION MODE] RPi.GPIO not found — running in simulation mode")

# --- Hardware Config ----------------------------------------------------------
SERVO_PIN = 13

SENSORS = [
    {'name': 'Center', 'TRIG': 4,  'ECHO': 17, 'angle': 90,  'direction': 'front'},
    {'name': 'Right',  'TRIG': 5,  'ECHO': 27, 'angle': 0,   'direction': 'right'},
    {'name': 'Left',   'TRIG': 18, 'ECHO': 25, 'angle': 180, 'direction': 'left'},
]

# --- State ---------------------------------------------------------------------
state = {
    'servo_angle': 90,
    'auto_mode': True,          # True = sensors control servo, False = manual
    'threshold_cm': 50,         # distance threshold to trigger servo
    'speed_ms': 12,             # delay (ms) per 1-degree step. 0 = instant, higher = slower
    'sensor_enabled': {s['name']: True for s in SENSORS},
    'sensor_distances': {s['name']: None for s in SENSORS},
    'triggered_by': None,       # which sensor last moved the servo
}

clients = set()

# Tracks the currently-running smooth-move task so a new target can cancel it
_move_task = None

# --- GPIO Setup ---------------------------------------------------------------
if not SIMULATION:
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(SERVO_PIN, GPIO.OUT)
    for s in SENSORS:
        GPIO.setup(s['TRIG'], GPIO.OUT)
        GPIO.setup(s['ECHO'], GPIO.IN)
    pwm = GPIO.PWM(SERVO_PIN, 50)
    pwm.start(0)

def _apply_pwm(angle):
    """Push a given angle out to the physical servo (no state mutation)."""
    if not SIMULATION:
        duty = 2 + (angle / 18)
        pwm.ChangeDutyCycle(duty)

async def move_servo_to(angle, broadcast_each_step=True):
    """
    Smoothly move the servo from its current angle to `angle`,
    stepping 1 degree at a time with a delay of state['speed_ms']
    between steps. Cancels any move already in progress.
    """
    global _move_task

    angle = max(0, min(180, int(angle)))

    # Cancel any in-flight movement so the new target takes over immediately
    if _move_task is not None and not _move_task.done():
        _move_task.cancel()
        try:
            await _move_task
        except asyncio.CancelledError:
            pass

    async def _runner():
        current = state['servo_angle']
        delay = state['speed_ms'] / 1000.0

        if delay <= 0 or current == angle:
            state['servo_angle'] = angle
            _apply_pwm(angle)
            if broadcast_each_step:
                await broadcast(build_state_msg())
            print(f"[SERVO] -> {angle}°")
            return

        step = 1 if angle > current else -1
        for a in range(current, angle, step):
            state['servo_angle'] = a
            _apply_pwm(a)
            if broadcast_each_step:
                await broadcast(build_state_msg())
            await asyncio.sleep(delay)

        state['servo_angle'] = angle
        _apply_pwm(angle)
        if broadcast_each_step:
            await broadcast(build_state_msg())
        print(f"[SERVO] -> {angle}° (smooth)")

    _move_task = asyncio.create_task(_runner())
    return _move_task

def read_distance(sensor):
    """Read distance from HC-SR04 ultrasonic sensor in cm."""
    if SIMULATION:
        import random
        return round(random.uniform(10, 120), 1)

    trig = sensor['TRIG']
    echo = sensor['ECHO']

    GPIO.output(trig, False)
    time.sleep(0.02)

    GPIO.output(trig, True)
    time.sleep(0.00001)
    GPIO.output(trig, False)

    pulse_start = time.time()
    timeout = pulse_start + 0.04

    while GPIO.input(echo) == 0:
        pulse_start = time.time()
        if pulse_start > timeout:
            return None

    pulse_end = time.time()
    timeout = pulse_end + 0.04
    while GPIO.input(echo) == 1:
        pulse_end = time.time()
        if pulse_end > timeout:
            return None

    duration = pulse_end - pulse_start
    distance = round(duration * 17150, 1)
    if distance < 2 or distance > 400:
        return None
    return distance

# --- Sensor Loop --------------------------------------------------------------
async def sensor_loop():
    """Continuously reads sensors and moves servo if auto mode is on."""
    while True:
        distances = {}
        triggered = None

        for s in SENSORS:
            name = s['name']
            if state['sensor_enabled'].get(name, True):
                dist = read_distance(s)
                distances[name] = dist

                # In auto mode: move servo toward first sensor within threshold
                if (
                    state['auto_mode']
                    and triggered is None
                    and dist is not None
                    and dist <= state['threshold_cm']
                ):
                    triggered = name
                    if state['servo_angle'] != s['angle']:
                        await move_servo_to(s['angle'], broadcast_each_step=True)
                        state['triggered_by'] = name
            else:
                distances[name] = None

        state['sensor_distances'] = distances
        if triggered is None:
            state['triggered_by'] = None

        # Broadcast state to all connected clients
        if clients:
            msg = build_state_msg()
            await broadcast(msg)

        await asyncio.sleep(0.15)

def build_state_msg():
    return json.dumps({
        'type': 'state',
        'servo_angle': state['servo_angle'],
        'auto_mode': state['auto_mode'],
        'threshold_cm': state['threshold_cm'],
        'speed_ms': state['speed_ms'],
        'sensor_enabled': state['sensor_enabled'],
        'sensor_distances': state['sensor_distances'],
        'triggered_by': state['triggered_by'],
    })

async def broadcast(msg):
    dead = set()
    for ws in clients:
        try:
            await ws.send(msg)
        except:
            dead.add(ws)
    clients.difference_update(dead)

# --- WebSocket Handler --------------------------------------------------------
async def handler(websocket):
    clients.add(websocket)
    print(f"[WS] Client connected — total: {len(clients)}")

    # Send current state immediately on connect
    await websocket.send(build_state_msg())

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
                cmd = msg.get('cmd')

                if cmd == 'set_angle':
                    angle = int(msg['angle'])
                    if not state['auto_mode']:
                        await move_servo_to(angle, broadcast_each_step=True)

                elif cmd == 'set_auto':
                    state['auto_mode'] = bool(msg['value'])
                    print(f"[MODE] auto={'ON' if state['auto_mode'] else 'OFF'}")

                elif cmd == 'set_threshold':
                    state['threshold_cm'] = max(5, min(400, int(msg['value'])))
                    print(f"[THRESHOLD] {state['threshold_cm']} cm")

                elif cmd == 'set_speed':
                    # speed_ms = delay in ms per 1-degree step (0 = instant)
                    state['speed_ms'] = max(0, min(100, int(msg['value'])))
                    print(f"[SPEED] {state['speed_ms']} ms/degree")

                elif cmd == 'toggle_sensor':
                    name = msg['name']
                    if name in state['sensor_enabled']:
                        state['sensor_enabled'][name] = bool(msg['value'])
                        print(f"[SENSOR] {name} = {'ON' if msg['value'] else 'OFF'}")

                # Immediately send updated state after any command
                await broadcast(build_state_msg())

            except Exception as e:
                print(f"[ERR] {e}")
                await websocket.send(json.dumps({'type': 'error', 'msg': str(e)}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(websocket)
        print(f"[WS] Client disconnected — total: {len(clients)}")

# --- Main ---------------------------------------------------------------------
async def main():
    print(f"[SERVER] Starting — SIMULATION={SIMULATION}")
    _apply_pwm(90)
    state['servo_angle'] = 90
    server = websockets.serve(handler, "0.0.0.0", 9999)
    async with server:
        print("[SERVER] Ready on ws://0.0.0.0:9999")
        await asyncio.gather(
            asyncio.Future(),   # run forever
            sensor_loop(),
        )

try:
    asyncio.run(main())
finally:
    if not SIMULATION:
        pwm.stop()
        GPIO.cleanup()
        print("[GPIO] Cleaned up")