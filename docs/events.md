# Events (demo-ready)

## alarm
- state: raised | reminder | cleared
- sensors: gas, pir (sound queda disponible pero no usado)

## security.intrusion
Se emite cuando:
- ARM == true
- door == OPEN
- motion == true

## control.arm_state
Toggle con touch rising edge.
Estados: ARMED / DISARMED

## aggregate
Cada ventana: light_avg, temp_c, humidity

## heartbeat
Cada intervalo: uptime, last_input_age, seq
