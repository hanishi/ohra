// The frames the server sends the pages, as examples/fastapi_relay_rally.py builds them.
// Times are on the server's clock, in seconds; x is a fraction of the court's width.

export type Player = {
  id: string;
  name: string;
  color: string;
  lives: number;
  connected: boolean;
  in_game: boolean;
  cpu: boolean;
  returns: number;
  best: number;
};

export type State = {
  type: "state";
  phase: "lobby" | "playing" | "over";
  players: Player[];
  ends_at: number | null;
  winners: string[];
  longest: number;
};

export type Incoming = { type: "incoming"; serial: number; t: number; x: number; vx: number; vy: number };
export type Drop = { type: "drop"; to: string; t: number; vy: number; speed: number; rally: number };
export type Pass = { type: "pass"; from: string; to: string; t0: number; t1: number };
export type You = { type: "you"; id: string; name: string; color: string };
export type Full = { type: "full" };

export type Frame = State | Incoming | Drop | Pass | You | Full;
