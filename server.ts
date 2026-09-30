import http from 'http';
import path from 'path';
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer as createViteServer } from 'vite';
import { Chess, Square } from 'chess.js';
import {
  Card,
  ClientMessage,
  GameLogEntry,
  GameState,
  PlayerColor,
  RoomPlayer,
  RoomState,
  ServerMessage,
  StandardPieceType,
  GameExpansionsConfig,
  AwakenedRookState,
  AwakeningAlertState,
  ChampionPiece,
} from './src/types.ts';
import {
  createInitialGameState,
  createSafeChess,
  drawCard,
  evaluateGameEnd,
  isSquareBlocked,
  getPushMoves,
  isCardPermittedForPiece,
  calculatePiecePoints,
  syncFenTurn,
  ensureEdgePawnsPromoted,
  createBoardWithMonolith,
  isKingChecked,
  getSurroundingSquares,
  getChampionDeploySquares,
  getChampionNormalMoves,
  getBerserkerUltimateMoves,
  getMusketeerSnipeTargets,
  getSummonerPlacementSquares,
  getNinjaAdjacentEnemyPieces,
  getClockBlockReason,
  isCastlingBlockedByRestrictedSquares,
  updateBeaconPieceSquare,
  swapBeaconPieceSquares,
  getLegalMovesForPiece,
  getQueenMovesForKing,
  getPieceCapturePoints,
  awardCaptureRefresh,
} from './src/utils/chessLogic.ts';

const app = express();
const PORT = 3000;
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());

// In-memory room store
const rooms = new Map<string, RoomState>();
const socketMeta = new Map<WebSocket, { roomId: string; playerId: string }>();

function generateRoomId(): string {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let result = '';
  for (let i = 0; i < 6; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

function getOrCreateRoom(roomId: string, mode?: 'online' | 'pass_and_play', hostId?: string): RoomState {
  const cleanId = roomId.trim().toUpperCase();
  let room = rooms.get(cleanId);
  if (!room) {
    room = {
      roomId: cleanId,
      players: {},
      gameState: createInitialGameState(),
      createdAt: Date.now(),
      mode: mode || 'online',
      lobbyConfig: {
        hostId: hostId || '',
        colorPreference: 'w',
        clockEnabled: false,
        initialMinutes: 5,
        incrementSeconds: 0,
        gameStarted: false,
        expansions: {
          shiftedParadigm: true,
          championsOfTheMonolith: true,
          theAwakening: true,
        },
      },
    };
    rooms.set(cleanId, room);
  } else {
    if (!room.mode && mode) room.mode = mode;
    if (!room.lobbyConfig) {
      room.lobbyConfig = {
        hostId: hostId || Object.keys(room.players)[0] || '',
        colorPreference: 'w',
        clockEnabled: false,
        initialMinutes: 5,
        incrementSeconds: 0,
        gameStarted: room.gameState.status === 'playing',
        expansions: {
          shiftedParadigm: true,
          championsOfTheMonolith: true,
          theAwakening: true,
        },
      };
    } else if (!room.lobbyConfig.expansions) {
      room.lobbyConfig.expansions = {
        shiftedParadigm: true,
        championsOfTheMonolith: true,
        theAwakening: true,
      };
    }
  }
  return room;
}

function broadcastRoom(roomId: string) {
  const room = rooms.get(roomId);
  if (!room) return;

  wss.clients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      const meta = socketMeta.get(ws);
      if (meta && meta.roomId === roomId) {
        const player = room.players[meta.playerId];
        const yourColor = player ? player.color : 'spectator';
        const msg: ServerMessage = {
          type: 'room_state',
          state: room,
          yourColor,
        };
        ws.send(JSON.stringify(msg));
      }
    }
  });
}

function sendToSocket(ws: WebSocket, msg: ServerMessage) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

// REST endpoints
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: Date.now() });
});

app.get('/api/rooms/:roomId', (req, res) => {
  const roomId = req.params.roomId.toUpperCase();
  const room = rooms.get(roomId);
  if (!room) {
    return res.status(404).json({ error: 'Room not found' });
  }
  res.json({ room });
});

app.post('/api/rooms', (req, res) => {
  const roomId = req.body.roomId ? req.body.roomId.toUpperCase() : generateRoomId();
  const mode = req.body.mode || 'online';
  const room = getOrCreateRoom(roomId, mode);
  res.json({ roomId: room.roomId });
});

// WebSocket message handling
wss.on('connection', (ws: WebSocket) => {
  ws.on('message', (data: string) => {
    try {
      const msg: ClientMessage = JSON.parse(data.toString());

      if (msg.type === 'ping') {
        sendToSocket(ws, { type: 'pong' });
        return;
      }

      if (msg.type === 'join_room' || msg.type === 'create_room') {
        const roomId = (msg.roomId || generateRoomId()).trim().toUpperCase();
        const mode = (msg as any).mode || 'online';
        const room = getOrCreateRoom(roomId, mode, msg.playerId);

        socketMeta.set(ws, { roomId, playerId: msg.playerId });

        // If no host recorded, set this player as host
        if (!room.lobbyConfig.hostId) {
          room.lobbyConfig.hostId = msg.playerId;
        }

        const isHost = room.lobbyConfig.hostId === msg.playerId;

        // Assign player color
        let assignedColor: PlayerColor | 'spectator' = 'spectator';

        if (room.mode === 'pass_and_play') {
          assignedColor = 'w';
          room.players[msg.playerId] = {
            id: msg.playerId,
            name: msg.playerName || 'Commander (Pass & Play)',
            color: 'w',
            connected: true,
            isHost: true,
          };
        } else {
          const existingPlayer = room.players[msg.playerId];
          if (existingPlayer) {
            existingPlayer.connected = true;
            existingPlayer.name = msg.playerName || existingPlayer.name;
            existingPlayer.isHost = isHost;
            assignedColor = existingPlayer.color;
          } else {
            // Count non-spectator players
            const nonSpectators = Object.values(room.players).filter((p) => p.color !== 'spectator');

            if (nonSpectators.length === 0) {
              // First player (Host)
              const pref = room.lobbyConfig.colorPreference;
              assignedColor = pref === 'b' ? 'b' : 'w';
            } else if (nonSpectators.length === 1) {
              // Second player (Opponent)
              const otherColor = nonSpectators[0].color;
              assignedColor = otherColor === 'w' ? 'b' : 'w';
            } else {
              assignedColor = 'spectator';
            }

            room.players[msg.playerId] = {
              id: msg.playerId,
              name: msg.playerName || `Commander ${Object.keys(room.players).length + 1}`,
              color: assignedColor,
              connected: true,
              isHost,
            };
          }
        }

        broadcastRoom(roomId);
        return;
      }

      const meta = socketMeta.get(ws);
      if (!meta) {
        sendToSocket(ws, { type: 'error', message: 'Not connected to a room' });
        return;
      }

      const room = rooms.get(meta.roomId);
      if (!room) {
        sendToSocket(ws, { type: 'error', message: 'Room not found' });
        return;
      }

      // Handle Player Rename
      if (msg.type === 'rename_player') {
        const cleanName = (msg.playerName || '').trim().slice(0, 24);
        if (cleanName) {
          const targetId = msg.playerId || meta.playerId;
          if (room.players[targetId]) {
            room.players[targetId].name = cleanName;
          }
          broadcastRoom(meta.roomId);
        }
        return;
      }

      // Handle Switch Mode request
      if (msg.type === 'set_room_mode') {
        room.mode = msg.mode;
        broadcastRoom(meta.roomId);
        return;
      }

      // Handle Update Lobby Configuration
      if (msg.type === 'update_lobby_config') {
        if (msg.colorPreference) {
          room.lobbyConfig.colorPreference = msg.colorPreference;
        }
        if (msg.clockEnabled !== undefined) {
          room.lobbyConfig.clockEnabled = msg.clockEnabled;
        }
        if (msg.initialMinutes !== undefined) {
          room.lobbyConfig.initialMinutes = msg.initialMinutes;
        }
        if (msg.incrementSeconds !== undefined) {
          room.lobbyConfig.incrementSeconds = msg.incrementSeconds;
        }
        if (msg.mode) {
          room.mode = msg.mode;
        }
        if (msg.expansions) {
          room.lobbyConfig.expansions = {
            ...room.lobbyConfig.expansions,
            ...msg.expansions,
          };
        }

        // Preview color assignment in lobby
        const playerIds = Object.keys(room.players);
        const hostId = room.lobbyConfig.hostId || playerIds[0];
        const opponentId = playerIds.find((id) => id !== hostId);

        const pref = room.lobbyConfig.colorPreference;
        if (pref === 'w') {
          if (room.players[hostId]) room.players[hostId].color = 'w';
          if (opponentId && room.players[opponentId]) room.players[opponentId].color = 'b';
        } else if (pref === 'b') {
          if (room.players[hostId]) room.players[hostId].color = 'b';
          if (opponentId && room.players[opponentId]) room.players[opponentId].color = 'w';
        } else {
          // 'random'
          if (room.players[hostId]) room.players[hostId].color = 'w';
          if (opponentId && room.players[opponentId]) room.players[opponentId].color = 'b';
        }

        broadcastRoom(meta.roomId);
        return;
      }

      // Handle Start Game from Lobby
      if (msg.type === 'start_game') {
        const pref = msg.colorPreference || room.lobbyConfig.colorPreference || 'w';
        const clockCfg = msg.clockConfig || {
          enabled: room.lobbyConfig.clockEnabled,
          initialMinutes: room.lobbyConfig.initialMinutes,
          incrementSeconds: room.lobbyConfig.incrementSeconds,
        };
        const expansions: GameExpansionsConfig = msg.expansions || room.lobbyConfig.expansions || {
          shiftedParadigm: true,
          championsOfTheMonolith: true,
          theAwakening: true,
        };

        // Create fresh starting game state with configured expansions
        const freshState = createInitialGameState(expansions);
        room.gameState = freshState;

        // Configure clock
        freshState.clock = {
          enabled: clockCfg.enabled,
          initialMinutes: clockCfg.initialMinutes,
          incrementSeconds: clockCfg.incrementSeconds,
          w: Math.round(clockCfg.initialMinutes * 60),
          b: Math.round(clockCfg.initialMinutes * 60),
          lastTick: clockCfg.enabled ? Date.now() : null,
          isRunning: Boolean(clockCfg.enabled),
        };

        // Resolve Color Selection:
        const playerIds = Object.keys(room.players);
        const hostId = room.lobbyConfig.hostId || playerIds[0];
        const opponentId = playerIds.find((id) => id !== hostId);

        let hostColor: PlayerColor = 'w';
        if (pref === 'random') {
          hostColor = Math.random() < 0.5 ? 'w' : 'b';
        } else {
          hostColor = pref;
        }
        const opponentColor: PlayerColor = hostColor === 'w' ? 'b' : 'w';

        if (room.players[hostId]) {
          room.players[hostId].color = hostColor;
        }
        if (opponentId && room.players[opponentId]) {
          room.players[opponentId].color = opponentColor;
        }

        // Set status to playing and start clock
        freshState.status = 'playing';
        room.lobbyConfig.gameStarted = true;
        room.lobbyConfig.colorPreference = pref;
        room.lobbyConfig.clockEnabled = clockCfg.enabled;
        room.lobbyConfig.initialMinutes = clockCfg.initialMinutes;
        room.lobbyConfig.incrementSeconds = clockCfg.incrementSeconds;
        room.lobbyConfig.expansions = expansions;

        const clockDesc = clockCfg.enabled
          ? `${clockCfg.initialMinutes}m + ${clockCfg.incrementSeconds}s`
          : 'No Clock';
        const hostName = room.players[hostId]?.name || 'Host';
        const oppName = opponentId && room.players[opponentId] ? room.players[opponentId].name : 'Opponent';
        const activeExpNames = [
          expansions.shiftedParadigm !== false ? 'Shifted Paradigm' : null,
          expansions.championsOfTheMonolith !== false ? 'Champions of the Monolith' : null,
          expansions.theAwakening !== false ? 'The Awakening' : null,
        ].filter(Boolean);
        const expansionTag = activeExpNames.length > 0 ? ` [${activeExpNames.join(', ')}]` : ' [Base Game]';

        freshState.historyLog = [
          {
            id: `start_${Date.now()}`,
            turn: 'w',
            turnNumber: 1,
            actionType: 'pass',
            detail: `Match started! ${hostName} (${hostColor.toUpperCase()}) vs ${oppName} (${opponentColor.toUpperCase()}) [${clockDesc}]${expansionTag}. White opens.`,
            timestamp: Date.now(),
            whitePoints: freshState.whitePoints,
            blackPoints: freshState.blackPoints,
          },
        ];

        broadcastRoom(meta.roomId);
        return;
      }

      // Handle Return to Lobby
      if (msg.type === 'return_to_lobby') {
        const expansions: GameExpansionsConfig = room.lobbyConfig.expansions || {
          shiftedParadigm: true,
          championsOfTheMonolith: true,
          theAwakening: true,
        };
        room.gameState = createInitialGameState(expansions);
        room.gameState.status = 'lobby';
        room.lobbyConfig.gameStarted = false;
        broadcastRoom(meta.roomId);
        return;
      }

      const player = room.players[meta.playerId];
      const state = room.gameState;

      // In online mode, verify player is active turn
      if (room.mode === 'online' && player && player.color !== 'spectator') {
        const exemptTypes = ['restart_game', 'start_game', 'update_lobby_config', 'return_to_lobby', 'set_room_mode'];
        if (player.color !== state.turn && !exemptTypes.includes(msg.type)) {
          sendToSocket(ws, { type: 'error', message: "It is not your turn!" });
          return;
        }
      }

      // Handle Game Actions
      handleGameAction(room, msg, player ? player.color : state.turn, ws);
      broadcastRoom(meta.roomId);
    } catch (err: any) {
      console.error('WebSocket error:', err);
      sendToSocket(ws, { type: 'error', message: err?.message || 'Server error' });
    }
  });

  ws.on('close', () => {
    const meta = socketMeta.get(ws);
    if (meta) {
      const room = rooms.get(meta.roomId);
      if (room && room.players[meta.playerId]) {
        room.players[meta.playerId].connected = false;
        broadcastRoom(meta.roomId);
      }
      socketMeta.delete(ws);
    }
  });
});

function consumeCardAndRefill(
  state: GameState,
  currentTurn: PlayerColor,
  cardPlayed: Card | null,
  isExhausted: boolean = false
) {
  if (cardPlayed) {
    if (isExhausted) {
      if (!state.exhausted) state.exhausted = [];
      state.exhausted.push(cardPlayed);
    } else {
      const isJokerWild = cardPlayed.isJokerWild || (state.jokerLocked && state.jokerLocked[currentTurn]);
      if (!isJokerWild) {
        state.discard.push(cardPlayed);
      }
    }
  }

  if (state.jokerLocked && state.jokerLocked[currentTurn]) {
    // Cannot draw until all wild cards in hand are exhausted
    if (state.hands[currentTurn].length === 0) {
      state.jokerLocked[currentTurn] = false;
      // Draw 5 new cards from the deck
      for (let i = 0; i < 5; i++) {
        const drawRes = drawCard(state);
        if (drawRes.drawnCard) state.hands[currentTurn].push(drawRes.drawnCard);
        state.deck = drawRes.newDeck;
        state.discard = drawRes.newDiscard;
        if (drawRes.reshuffled) state.powerUps = drawRes.powerUps;
      }
      state.historyLog.unshift({
        id: `log_${Date.now()}_${Math.random()}`,
        turn: currentTurn,
        turnNumber: state.turnNumber,
        actionType: 'move',
        cardName: 'Hand Refilled',
        detail: `${currentTurn === 'w' ? 'White' : 'Black'} exhausted all Joker Wilds and drew 5 new cards from the deck!`,
        timestamp: Date.now(),
        whitePoints: state.whitePoints,
        blackPoints: state.blackPoints,
      });
    }
  } else {
    // Draw cards until player has 5 cards in hand
    while (state.hands[currentTurn].length < 5) {
      const drawRes = drawCard(state);
      if (drawRes.drawnCard) state.hands[currentTurn].push(drawRes.drawnCard);
      state.deck = drawRes.newDeck;
      state.discard = drawRes.newDiscard;
      if (drawRes.reshuffled) state.powerUps = drawRes.powerUps;
      if (!drawRes.drawnCard) break;
    }
  }
}

function cleanMonolithAndChampions(
  board: Chess,
  monolithSquare: string | null,
  champions?: { w: ChampionPiece | null; b: ChampionPiece | null } | null,
  landedSquare?: string
) {
  const epSquareBeforeRemove = (board as any)._epSquare;
  if (monolithSquare && monolithSquare !== landedSquare) {
    board.remove(monolithSquare as Square);
  }
  if (champions) {
    for (const side of ['w', 'b'] as PlayerColor[]) {
      const champ = champions[side];
      if (champ?.square && champ.square !== landedSquare) {
        board.remove(champ.square as Square);
      }
    }
  }
  // Remove all placeholder squares tracked during createBoardWithMonolith or temporary placements
  const placeholderSquares: string[] = (board as any)._placeholderSquares || [];
  for (const sq of placeholderSquares) {
    if (sq !== landedSquare) {
      board.remove(sq as Square);
    }
  }
  if (epSquareBeforeRemove !== undefined && epSquareBeforeRemove !== -1) {
    (board as any)._epSquare = epSquareBeforeRemove;
  }
}

function advanceTurnState(state: GameState, currentTurn: PlayerColor, nextTurn: PlayerColor) {
  // Snapshot board history for Clock rewind
  if (!state.boardHistory) state.boardHistory = [];
  state.boardHistory.push({
    fen: state.fen,
    monolithSquare: state.monolithSquare,
    restrictedSquares: [...state.restrictedSquares],
    champions: {
      w: state.champions?.w ? { ...state.champions.w } : null,
      b: state.champions?.b ? { ...state.champions.b } : null,
    },
    superPawns: [...(state.superPawns || [])],
    superKnights: [...(state.superKnights || [])],
    awakenedRooks: state.awakenedRooks ? state.awakenedRooks.map((r) => ({ ...r })) : [],
  });
  if (state.boardHistory.length > 100) {
    state.boardHistory.shift();
  }

  // Red aura expiration: Remove after your opponent moves after you played an awakened card
  if (state.awakenedByPlayer && currentTurn !== state.awakenedByPlayer) {
    state.awakenedPieceSquare = null;
    state.awakenedByPlayer = null;
    state.awakenedOnTurnNumber = null;
    state.awakeningAlert = null;
  } else if (
    state.awakenedOnTurnNumber !== null &&
    state.awakenedOnTurnNumber !== undefined &&
    state.turnNumber >= state.awakenedOnTurnNumber + 1 &&
    currentTurn !== state.awakenedByPlayer
  ) {
    state.awakenedPieceSquare = null;
    state.awakenedByPlayer = null;
    state.awakenedOnTurnNumber = null;
    state.awakeningAlert = null;
  }

  state.turn = nextTurn;
  state.turnNumber += 1;
  state.visionActive[currentTurn] = false;

  // Beacon expiration check (lasts 4 turns: activation turn, opp turn, your turn, opp turn)
  if (state.beaconEffect && state.turnNumber >= state.beaconEffect.expiresOnTurnNumber) {
    state.beaconEffect = null;
  }

  // Awakened Rooks countdown: counts down each turn starting with the opponent's turn (5 turns total)
  if (state.awakenedRooks && state.awakenedRooks.length > 0) {
    const activeRooks: AwakenedRookState[] = [];
    for (const rook of state.awakenedRooks) {
      const nextTurns = rook.turnsRemaining - 1;
      if (nextTurns > 0) {
        activeRooks.push({
          ...rook,
          turnsRemaining: nextTurns,
        });
      }
    }
    state.awakenedRooks = activeRooks;
  }

  // Static Card: check if stun procs at the 3rd turn (your next turn) and lasts through opponent's turn
  if (state.staticEffect && state.staticEffect.pending && state.turnNumber >= (state.staticEffect.procOnTurnNumber || 0)) {
    state.staticEffect.pending = false;
    state.staticEffect.active = true;
    state.staticEffect.stunnedSquares = state.monolithSquare ? getSurroundingSquares(state.monolithSquare) : [];
    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: state.turn,
      turnNumber: state.turnNumber,
      actionType: 'static',
      cardName: 'Static Stun Procs',
      detail: `⚡ STATIC STUN DISCHARGED! Electrifying shockwave stuns all units around the Monolith (active for both your turn and opponent's turn)!`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });
  } else if (state.staticEffect && state.staticEffect.active && state.turnNumber >= state.staticEffect.expiresOnTurnNumber) {
    state.staticEffect = null;
  }

  // Check Game End / Checkmate condition for the defending player (nextTurn)
  if (state.status === 'playing') {
    const evalChess = createSafeChess(state.fen);
    const evalRes = evaluateGameEnd(evalChess, state);
    if (evalRes.status !== 'playing') {
      state.status = evalRes.status;
      state.winner = evalRes.winner;
      state.winReason = evalRes.winReason;
    }
    state.whitePoints = evalRes.whitePoints;
    state.blackPoints = evalRes.blackPoints;
  }

  if (state.clock && state.clock.enabled && state.status === 'playing') {
    if (!state.clock.isRunning) {
      state.clock.isRunning = true;
      state.clock.lastTick = Date.now();
    } else if (state.clock.lastTick) {
      const elapsed = (Date.now() - state.clock.lastTick) / 1000;
      state.clock[currentTurn] = Math.max(0, Math.round(state.clock[currentTurn] - elapsed + state.clock.incrementSeconds));
      state.clock.lastTick = Date.now();

      if (state.clock[currentTurn] <= 0) {
        state.clock[currentTurn] = 0;
        state.status = 'timeout';
        state.winner = nextTurn;
        state.winReason = `Time Expired: ${currentTurn === 'w' ? 'White' : 'Black'} ran out of time! ${nextTurn === 'w' ? 'White' : 'Black'} wins.`;
      }
    }
  }
}

function handleGameAction(
  room: RoomState,
  msg: ClientMessage,
  actorColor: PlayerColor | 'spectator',
  ws?: WebSocket
) {
  const state = room.gameState;
  const currentTurn = state.turn;
  let chess = createSafeChess(state.fen);

  if (msg.type === 'restart_game') {
    const prevClock = room.gameState.clock ? { ...room.gameState.clock } : null;
    const currentExp: GameExpansionsConfig = room.gameState.expansions || room.lobbyConfig.expansions || {
      shiftedParadigm: true,
      championsOfTheMonolith: true,
    };
    room.gameState = createInitialGameState(currentExp);
    if (prevClock) {
      room.gameState.clock = {
        enabled: prevClock.enabled,
        initialMinutes: prevClock.initialMinutes,
        incrementSeconds: prevClock.incrementSeconds,
        w: prevClock.initialMinutes * 60,
        b: prevClock.initialMinutes * 60,
        lastTick: null,
        isRunning: false,
      };
    }
    return;
  }

  if (msg.type === 'update_clock_config') {
    if (!state.clock) {
      state.clock = {
        enabled: msg.enabled,
        initialMinutes: msg.initialMinutes,
        incrementSeconds: msg.incrementSeconds,
        w: msg.initialMinutes * 60,
        b: msg.initialMinutes * 60,
        lastTick: null,
        isRunning: false,
      };
    } else {
      state.clock.enabled = msg.enabled;
      state.clock.initialMinutes = msg.initialMinutes;
      state.clock.incrementSeconds = msg.incrementSeconds;
      state.clock.w = msg.initialMinutes * 60;
      state.clock.b = msg.initialMinutes * 60;
      state.clock.lastTick = null;
      state.clock.isRunning = false;
    }
    return;
  }

  if (state.status !== 'playing') {
    return;
  }

  if (msg.type === 'play_joker') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'joker');
    if (cardIdx === -1) return;

    const currentHand = state.hands[currentTurn];
    // Discard all other cards and the Joker card itself
    for (const c of currentHand) {
      if (!c.isJokerWild) {
        state.discard.push(c);
      }
    }

    // Change all cards in hand to wild
    const wildCount = Math.max(5, currentHand.length);
    const wilds: Card[] = [];
    for (let i = 0; i < wildCount; i++) {
      wilds.push({
        id: `joker_wild_${Date.now()}_${i}_${Math.random().toString(36).substring(2, 6)}`,
        type: 'wild',
        name: 'Joker Wild',
        description: 'Move any piece. Vanishes on use (not sent to discard). Cannot draw until exhausted.',
        isJokerWild: true,
      });
    }

    state.hands[currentTurn] = wilds;
    state.jokerLocked[currentTurn] = true;

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'joker',
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'joker',
      cardName: 'Joker Card',
      detail: `${currentTurn === 'w' ? 'White' : 'Black'} played Joker! Hand sent to discard and converted into ${wildCount} Wild cards. Card draws locked until exhausted. (Free action — Turn continues!)`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });

    // Does not count as turn
    return;
  }

  if (msg.type === 'play_beacon') {
    if (!state.monolithSquare) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Cannot activate Beacon: There is no Monolith on the board!' });
      return;
    }
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'beacon');
    if (cardIdx === -1) return;

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    state.discard.push(cardPlayed);

    const protectedSquares = getSurroundingSquares(state.monolithSquare);

    // Beacon buff lasts 4 turns (your turn activation, opp's turn, your next turn, opp's next turn)
    // Only units surrounding the Monolith receive the invulnerability buff.
    // If pieces leave the field or the Monolith changes position, they will not be invulnerable anymore.
    state.beaconEffect = {
      active: true,
      protectedSquares,
      expiresOnTurnNumber: state.turnNumber + 4,
    };

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'beacon',
      monolithSquare: state.monolithSquare,
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'beacon',
      cardName: 'Beacon Card',
      detail: `✨ ${currentTurn === 'w' ? 'White' : 'Black'} activated Beacon! Holy Sanctum protects all units surrounding the Monolith for 4 turns. (Leaving the field or moving the Monolith removes invulnerability!) (Free action — Turn continues!)`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });
    return;
  }

  if (msg.type === 'play_static') {
    if (!state.monolithSquare) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Cannot activate Static: There is no Monolith on the board!' });
      return;
    }
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'static');
    if (cardIdx === -1) return;

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    state.discard.push(cardPlayed);

    // Static stun procs at the 3rd turn (your next turn when you activated the static) and lasts through the opponent's turn
    // (turn: activation, turn+1: opp 1st turn charging, turn+2: your 2nd turn PROCS stun, turn+3: opp 2nd turn STUNNED, turn+4: expires)
    state.staticEffect = {
      active: false,
      pending: true,
      casterColor: currentTurn,
      procOnTurnNumber: state.turnNumber + 2,
      expiresOnTurnNumber: state.turnNumber + 4,
      stunnedSquares: [],
    };

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'static',
      monolithSquare: state.monolithSquare,
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'static',
      cardName: 'Static Card Activated',
      detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} activated Static Card! Electrical surge is charging around the Monolith and will stun all units on the 3rd turn (your next turn) and through the opponent's turn! (Free action — Turn continues!)`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });
    return;
  }

  if (msg.type === 'play_clock') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'clock');
    if (cardIdx === -1) return;

    const blockReason = getClockBlockReason(state, currentTurn);
    if (blockReason) {
      if (ws) {
        sendToSocket(ws, {
          type: 'error',
          message: blockReason,
        });
      }
      return;
    }

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    state.discard.push(cardPlayed);

    // Revert board 2 turns (your opponent's previous move and your previous move)
    // In boardHistory: index 0 is game start. If length is 3 (turns 1 and 2 completed),
    // target index is length - 1 - 2 = 0.
    const targetIndex = state.boardHistory ? state.boardHistory.length - 1 - 2 : -1;
    if (targetIndex >= 0 && state.boardHistory && state.boardHistory[targetIndex]) {
      const targetSnapshot = state.boardHistory[targetIndex];

      state.fen = syncFenTurn(targetSnapshot.fen, currentTurn);
      state.monolithSquare = targetSnapshot.monolithSquare;
      state.restrictedSquares = [...targetSnapshot.restrictedSquares];
      state.champions = {
        w: targetSnapshot.champions.w ? { ...targetSnapshot.champions.w } : null,
        b: targetSnapshot.champions.b ? { ...targetSnapshot.champions.b } : null,
      };

      // Super Pawn permanence: Clock cannot revert Super Pawn back to normal pawn!
      const rewoundChess = createSafeChess(state.fen);
      const superPawnCandidates = new Set<string>([
        ...(targetSnapshot.superPawns || []),
        ...(state.superPawns || []),
      ]);
      if (state.boardHistory) {
        for (const snap of state.boardHistory) {
          if (snap.superPawns) {
            for (const sq of snap.superPawns) {
              superPawnCandidates.add(sq);
            }
          }
        }
      }
      const validSuperPawns: string[] = [];
      for (let r = 1; r <= 8; r++) {
        for (const f of ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']) {
          const sq = `${f}${r}`;
          const p = rewoundChess.get(sq as Square);
          if (p && p.type === 'p' && superPawnCandidates.has(sq)) {
            validSuperPawns.push(sq);
          }
        }
      }
      // If a candidate Super Pawn is not at its previous square because it moved, preserve it on its file
      for (const sq of superPawnCandidates) {
        if (!validSuperPawns.includes(sq)) {
          const file = sq[0];
          for (let rank = 1; rank <= 8; rank++) {
            const checkSq = `${file}${rank}`;
            const p = rewoundChess.get(checkSq as Square);
            if (p && p.type === 'p' && !validSuperPawns.includes(checkSq)) {
              validSuperPawns.push(checkSq);
              break;
            }
          }
        }
      }
      state.superPawns = validSuperPawns;

      // Awakened Rook invulnerability must still take effect!
      if (state.awakenedRooks && state.awakenedRooks.length > 0) {
        const preservedRooks: AwakenedRookState[] = [];
        for (const r of state.awakenedRooks) {
          const snapRook = targetSnapshot.awakenedRooks?.find((tr) => tr.color === r.color);
          const rookSq = snapRook?.square || r.square;
          const p = rewoundChess.get(rookSq as Square);
          if (p && p.type === 'r' && p.color === r.color) {
            preservedRooks.push({
              ...r,
              square: rookSq,
              turnsRemaining: Math.max(r.turnsRemaining, snapRook?.turnsRemaining || r.turnsRemaining),
            });
          } else {
            let foundSq = rookSq;
            for (let rank = 1; rank <= 8; rank++) {
              for (const file of ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']) {
                const s = `${file}${rank}`;
                const rp = rewoundChess.get(s as Square);
                if (rp && rp.type === 'r' && rp.color === r.color) {
                  foundSq = s;
                  break;
                }
              }
            }
            preservedRooks.push({
              ...r,
              square: foundSq,
            });
          }
        }
        state.awakenedRooks = preservedRooks;
      } else if (targetSnapshot.awakenedRooks && targetSnapshot.awakenedRooks.length > 0) {
        state.awakenedRooks = targetSnapshot.awakenedRooks.map((r) => ({ ...r }));
      }

      chess = createSafeChess(state.fen);
      state.whitePoints = calculatePiecePoints(chess, 'w');
      state.blackPoints = calculatePiecePoints(chess, 'b');

      // Truncate board history to the reverted snapshot
      state.boardHistory = state.boardHistory.slice(0, targetIndex + 1);
      state.turnNumber = Math.max(1, state.turnNumber - 2);
      state.awakenedPieceSquare = null;
      state.awakenedByPlayer = null;
      state.awakenedOnTurnNumber = null;
      state.awakeningAlert = null;
    }

    state.clockAlert = {
      player: currentTurn,
      message: `⏰ TIME REVERSED! ${currentTurn === 'w' ? 'White' : 'Black'} activated the Clock! Reverted 2 turns (opponent's previous move and your previous move).`,
      timestamp: Date.now(),
    };

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'clock',
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'clock',
      cardName: 'Activate the Clock',
      detail: `⏳ ${currentTurn === 'w' ? 'White' : 'Black'} activated the Clock! Reverted 2 turns (opponent's previous move and your previous move). (Free action — Turn continues!)`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });
    return;
  }

  if (msg.type === 'dismiss_clock_alert') {
    state.clockAlert = null;
    return;
  }

  if (msg.type === 'dismiss_pass_redraw_alert') {
    state.passRedrawAlert = null;
    return;
  }

  if (msg.type === 'dismiss_awakening_alert') {
    state.awakeningAlert = null;
    return;
  }

  // === THE AWAKENING EXPANSION HANDLERS ===
  if (msg.type === 'play_awakening_pawn') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'awakening_pawn');
    if (cardIdx === -1) return;

    const piece = chess.get(msg.targetSquare as Square);
    if (!piece || piece.color !== currentTurn || piece.type !== 'p') {
      if (ws) sendToSocket(ws, { type: 'error', message: 'You must select one of your Pawns to awaken into a Super Pawn!' });
      return;
    }

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed, true); // EXHAUSTED: will not reshuffle

    if (!state.superPawns) state.superPawns = [];
    if (!state.superPawns.includes(msg.targetSquare)) {
      state.superPawns.push(msg.targetSquare);
    }
    state.awakenedPieceSquare = msg.targetSquare;
    state.awakenedByPlayer = currentTurn;
    state.awakenedOnTurnNumber = state.turnNumber;

    state.awakeningAlert = {
      player: currentTurn,
      cardType: 'awakening_pawn',
      cardName: 'Awakening: Pawn',
      effectTitle: 'Super Pawn Awakened!',
      effectDescription: `${currentTurn === 'w' ? 'White' : 'Black'} permanently upgraded a pawn on ${msg.targetSquare} into a Super Pawn! Super Pawns can now also capture pieces directly in front of them.`,
      pieceSquare: msg.targetSquare,
      timestamp: Date.now(),
    };

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'awakening',
      from: msg.targetSquare,
      to: msg.targetSquare,
      awakeningType: 'awakening_pawn',
      awakeningDetails: `Pawn on ${msg.targetSquare} upgraded to Super Pawn`,
    };

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn, true);
    state.lastMovedPiece[currentTurn] = 'p';
    state.lastMovedSquare[currentTurn] = msg.targetSquare;
    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'awakening',
      cardName: 'Awakening: Pawn',
      detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} awoke Super Pawn on ${msg.targetSquare}! [Card Exhausted]`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_awakening_rook') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'awakening_rook');
    if (cardIdx === -1) return;

    const piece = chess.get(msg.from as Square);
    if (!piece || piece.color !== currentTurn || piece.type !== 'r') {
      if (ws) sendToSocket(ws, { type: 'error', message: 'You must select one of your Rooks to awaken!' });
      return;
    }

    const otherPlayer = currentTurn === 'w' ? 'b' : 'w';
    const validMoves = getLegalMovesForPiece(
      msg.from as Square,
      chess,
      state.restrictedSquares,
      state.monolithSquare,
      state.staticEffect?.stunnedSquares || [],
      state.beaconEffect?.active && state.monolithSquare ? getSurroundingSquares(state.monolithSquare) : [],
      state.champions ? state.champions[currentTurn]?.square || null : null,
      state.champions ? state.champions[otherPlayer] : null,
      state.champions,
      state.awakenedRooks || [],
      state.superPawns || []
    );

    if (!validMoves.includes(msg.to)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Invalid destination for Rook move!' });
      return;
    }

    // Execute move
    const capturedPiece = chess.get(msg.to as Square);
    const capturedChamp = Boolean(state.champions && state.champions[otherPlayer]?.square === msg.to);
    let earnedCapturePoints = (capturedPiece ? getPieceCapturePoints(capturedPiece.type) : 0) + (capturedChamp ? 5 : 0);
    const didCapture = Boolean(capturedPiece) || capturedChamp;
    if (capturedChamp && state.champions) {
      state.champions[otherPlayer] = null;
    }

    const boardWithMonolith = createBoardWithMonolith(state.fen, state.monolithSquare, currentTurn, state.champions);

    let moveResult: any = null;
    try {
      moveResult = boardWithMonolith.move({ from: msg.from as Square, to: msg.to as Square });
    } catch (e) {
      return;
    }
    if (!moveResult) return;

    cleanMonolithAndChampions(boardWithMonolith, state.monolithSquare, state.champions, msg.to);
    chess = boardWithMonolith;

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed, true); // EXHAUSTED

    if (!state.awakenedRooks) state.awakenedRooks = [];
    state.awakenedRooks = state.awakenedRooks.filter((r) => r.square !== msg.from);
    state.awakenedRooks.push({
      square: msg.to,
      color: currentTurn,
      turnsRemaining: 5,
      hasCaptured: didCapture,
    });
    state.awakenedPieceSquare = msg.to;
    state.awakenedByPlayer = currentTurn;
    state.awakenedOnTurnNumber = state.turnNumber;

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn, true);
    state.lastMovedPiece[currentTurn] = 'r';
    state.lastMovedSquare[currentTurn] = msg.to;

    state.awakeningAlert = {
      player: currentTurn,
      cardType: 'awakening_rook',
      cardName: 'Awakening: Rook',
      effectTitle: 'Shielded Rook Awakened!',
      effectDescription: `${currentTurn === 'w' ? 'White' : 'Black'} moved Rook to ${msg.to}. It is uncapturable for 5 turns and can only capture once while awakened!`,
      pieceSquare: msg.to,
      timestamp: Date.now(),
    };

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'awakening',
      from: msg.from,
      to: msg.to,
      awakeningType: 'awakening_rook',
      awakeningDetails: `Rook moved to ${msg.to} (5 turns invulnerable)`,
    };

    // Capture-Based Tactical Power-Up Refresh
    const { refreshed } = awardCaptureRefresh(state, currentTurn, earnedCapturePoints);
    let refreshDetail = '';
    if (refreshed.length > 0) {
      refreshDetail = ` | ⚡ Refreshed: ${refreshed.map((r) => `${r.name} (now x${r.newCount})`).join(', ')}`;
      for (const r of refreshed) {
        state.historyLog.unshift({
          id: `log_${Date.now()}_refresh_${Math.random()}`,
          turn: currentTurn,
          turnNumber: state.turnNumber,
          actionType: 'powerup_refresh',
          cardName: 'Tactical Power-Up Refresh',
          detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} refreshed ${r.name} (+1 stack, now x${r.newCount})!`,
          timestamp: Date.now(),
          whitePoints: calculatePiecePoints(chess, 'w', state.champions),
          blackPoints: calculatePiecePoints(chess, 'b', state.champions),
        });
      }
    }

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'awakening',
      cardName: 'Awakening: Rook',
      detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} awoke Rook on ${msg.to}! Shielded for 5 turns. [Card Exhausted]${refreshDetail}`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w', state.champions),
      blackPoints: calculatePiecePoints(chess, 'b', state.champions),
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_awakening_knight') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'awakening_knight');
    if (cardIdx === -1) return;

    const fromSquare = (msg as any).targetSquare || msg.from;
    const piece = chess.get(fromSquare as Square);
    if (!piece || piece.color !== currentTurn || piece.type !== 'n') {
      if (ws) sendToSocket(ws, { type: 'error', message: 'You must select one of your Knights to awaken!' });
      return;
    }

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed, true); // EXHAUSTED

    if (!state.superKnights) state.superKnights = [];

    // Case 1: In-place transformation without moving
    if (!msg.to || msg.to === fromSquare) {
      if (!state.superKnights.includes(fromSquare)) {
        state.superKnights.push(fromSquare);
      }
      state.awakenedPieceSquare = fromSquare;
      state.awakenedByPlayer = currentTurn;
      state.awakenedOnTurnNumber = state.turnNumber;

      state.awakeningAlert = {
        player: currentTurn,
        cardType: 'awakening_knight',
        cardName: 'Awakening: Knight',
        effectTitle: 'Super Knight Awakened!',
        effectDescription: `${currentTurn === 'w' ? 'White' : 'Black'} transformed Knight on ${fromSquare} into a Super Knight! It can move twice in a turn.`,
        pieceSquare: fromSquare,
        timestamp: Date.now(),
      };

      state.lastMoveDetails = {
        player: currentTurn,
        actionType: 'awakening',
        from: fromSquare,
        to: fromSquare,
        awakeningType: 'awakening_knight',
        awakeningDetails: `Knight on ${fromSquare} upgraded to Super Knight`,
      };

      const nextTurn = currentTurn === 'w' ? 'b' : 'w';
      state.fen = syncFenTurn(state.fen, nextTurn);
      state.lastMovedPiece[currentTurn] = 'n';
      state.lastMovedSquare[currentTurn] = fromSquare;

      state.historyLog.unshift({
        id: `log_${Date.now()}_${Math.random()}`,
        turn: currentTurn,
        turnNumber: state.turnNumber,
        actionType: 'awakening',
        cardName: 'Awakening: Knight',
        detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} awoke Super Knight on ${fromSquare}! [Card Exhausted]`,
        timestamp: Date.now(),
        whitePoints: state.whitePoints,
        blackPoints: state.blackPoints,
      });

      advanceTurnState(state, currentTurn, nextTurn);
      return;
    }

    // Case 2: Leaping move with optional double move
    const otherPlayer = currentTurn === 'w' ? 'b' : 'w';
    const validMoves1 = getLegalMovesForPiece(
      fromSquare as Square,
      chess,
      state.restrictedSquares,
      state.monolithSquare,
      state.staticEffect?.stunnedSquares || [],
      state.beaconEffect?.active && state.monolithSquare ? getSurroundingSquares(state.monolithSquare) : [],
      state.champions ? state.champions[currentTurn]?.square || null : null,
      state.champions ? state.champions[otherPlayer] : null,
      state.champions,
      state.awakenedRooks || [],
      state.superPawns || []
    );

    if (!validMoves1.includes(msg.to)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Invalid destination for Knight move!' });
      return;
    }

    const move1Piece = chess.get(msg.to as Square);
    const move1Champ = Boolean(state.champions && state.champions[otherPlayer]?.square === msg.to);
    let earnedCapturePoints = (move1Piece ? getPieceCapturePoints(move1Piece.type) : 0) + (move1Champ ? 5 : 0);
    const move1Captured = Boolean(move1Piece) || move1Champ;
    if (state.champions && state.champions[otherPlayer]?.square === msg.to) {
      state.champions[otherPlayer] = null;
    }

    // Execute Move 1
    const boardWithMonolith = createBoardWithMonolith(state.fen, state.monolithSquare, currentTurn, state.champions);
    try {
      boardWithMonolith.move({ from: fromSquare as Square, to: msg.to as Square });
    } catch (e) {
      return;
    }
    cleanMonolithAndChampions(boardWithMonolith, state.monolithSquare, state.champions, msg.to);
    chess = boardWithMonolith;

    let finalSquare = msg.to;
    let usedDoubleMove = false;

    // Optional Move 2
    if (msg.secondTo) {
      const validMoves2 = getLegalMovesForPiece(
        msg.to as Square,
        chess,
        state.restrictedSquares,
        state.monolithSquare,
        state.staticEffect?.stunnedSquares || [],
        state.beaconEffect?.active && state.monolithSquare ? getSurroundingSquares(state.monolithSquare) : [],
        state.champions ? state.champions[currentTurn]?.square || null : null,
        state.champions ? state.champions[otherPlayer] : null,
        state.champions,
        state.awakenedRooks || [],
        state.superPawns || []
      );

      // Super Knight can only capture once across both moves
      const p2 = chess.get(msg.secondTo as Square);
      const champ2 = Boolean(state.champions && state.champions[otherPlayer]?.square === msg.secondTo);
      const isSecondCapture = Boolean(p2) || champ2;
      if (!(move1Captured && isSecondCapture) && validMoves2.includes(msg.secondTo)) {
        if (isSecondCapture) {
          earnedCapturePoints += (p2 ? getPieceCapturePoints(p2.type) : 0) + (champ2 ? 5 : 0);
        }
        if (state.champions && state.champions[otherPlayer]?.square === msg.secondTo) {
          state.champions[otherPlayer] = null;
        }
        try {
          chess.move({ from: msg.to as Square, to: msg.secondTo as Square });
          finalSquare = msg.secondTo;
          usedDoubleMove = true;
        } catch (e) {
          // ignore move 2 failure
        }
      }
    }

    state.superKnights = state.superKnights.filter((sq) => sq !== fromSquare && sq !== msg.to && sq !== finalSquare);

    if (usedDoubleMove) {
      // Reverted to normal knight!
      state.awakenedPieceSquare = finalSquare;
      state.awakeningAlert = {
        player: currentTurn,
        cardType: 'awakening_knight',
        cardName: 'Awakening: Knight',
        effectTitle: 'Super Knight Double Move!',
        effectDescription: `${currentTurn === 'w' ? 'White' : 'Black'} executed a Double Move with Super Knight to ${finalSquare}, reverting it to a normal Knight!`,
        pieceSquare: finalSquare,
        timestamp: Date.now(),
      };
    } else {
      // Retains Super Knight status!
      state.superKnights.push(finalSquare);
      state.awakenedPieceSquare = finalSquare;
      state.awakeningAlert = {
        player: currentTurn,
        cardType: 'awakening_knight',
        cardName: 'Awakening: Knight',
        effectTitle: 'Super Knight Awakened!',
        effectDescription: `${currentTurn === 'w' ? 'White' : 'Black'} upgraded Knight on ${finalSquare} into a Super Knight! It can move twice in a turn.`,
        pieceSquare: finalSquare,
        timestamp: Date.now(),
      };
    }

    state.awakenedByPlayer = currentTurn;
    state.awakenedOnTurnNumber = state.turnNumber;

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn, true);
    state.lastMovedPiece[currentTurn] = 'n';
    state.lastMovedSquare[currentTurn] = finalSquare;

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'awakening',
      from: fromSquare,
      to: finalSquare,
      awakeningType: 'awakening_knight',
      awakeningDetails: usedDoubleMove ? `Double move to ${finalSquare}` : `Upgraded to Super Knight on ${finalSquare}`,
    };

    // Capture-Based Tactical Power-Up Refresh
    const { refreshed } = awardCaptureRefresh(state, currentTurn, earnedCapturePoints);
    let refreshDetail = '';
    if (refreshed.length > 0) {
      refreshDetail = ` | ⚡ Refreshed: ${refreshed.map((r) => `${r.name} (now x${r.newCount})`).join(', ')}`;
      for (const r of refreshed) {
        state.historyLog.unshift({
          id: `log_${Date.now()}_refresh_${Math.random()}`,
          turn: currentTurn,
          turnNumber: state.turnNumber,
          actionType: 'powerup_refresh',
          cardName: 'Tactical Power-Up Refresh',
          detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} refreshed ${r.name} (+1 stack, now x${r.newCount})!`,
          timestamp: Date.now(),
          whitePoints: calculatePiecePoints(chess, 'w', state.champions),
          blackPoints: calculatePiecePoints(chess, 'b', state.champions),
        });
      }
    }

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'awakening',
      cardName: 'Awakening: Knight',
      detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} awoke Knight to ${finalSquare}${usedDoubleMove ? ' (Double Move - Reverted)' : ' (Super Knight)'}! [Card Exhausted]${refreshDetail}`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w', state.champions),
      blackPoints: calculatePiecePoints(chess, 'b', state.champions),
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_awakening_bishop') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'awakening_bishop');
    if (cardIdx === -1) return;

    const piece = chess.get(msg.from as Square);
    if (!piece || piece.color !== currentTurn || piece.type !== 'b') {
      if (ws) sendToSocket(ws, { type: 'error', message: 'You must select one of your Bishops to awaken!' });
      return;
    }

    const otherPlayer = currentTurn === 'w' ? 'b' : 'w';
    const validMoves = getLegalMovesForPiece(
      msg.from as Square,
      chess,
      state.restrictedSquares,
      state.monolithSquare,
      state.staticEffect?.stunnedSquares || [],
      state.beaconEffect?.active && state.monolithSquare ? getSurroundingSquares(state.monolithSquare) : [],
      state.champions ? state.champions[currentTurn]?.square || null : null,
      state.champions ? state.champions[otherPlayer] : null,
      state.champions,
      state.awakenedRooks || [],
      state.superPawns || []
    );

    if (!validMoves.includes(msg.to)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Invalid destination for Bishop move!' });
      return;
    }

    const capturedPiece = chess.get(msg.to as Square);
    const capturedChamp = Boolean(state.champions && state.champions[otherPlayer]?.square === msg.to);
    let earnedCapturePoints = (capturedPiece ? getPieceCapturePoints(capturedPiece.type) : 0) + (capturedChamp ? 5 : 0);

    if (state.champions && state.champions[otherPlayer]?.square === msg.to) {
      state.champions[otherPlayer] = null;
    }

    const boardWithMonolith = createBoardWithMonolith(state.fen, state.monolithSquare, currentTurn, state.champions);
    try {
      boardWithMonolith.move({ from: msg.from as Square, to: msg.to as Square });
    } catch (e) {
      return;
    }
    cleanMonolithAndChampions(boardWithMonolith, state.monolithSquare, state.champions, msg.to);
    chess = boardWithMonolith;

    // Revive piece adjacent to bishop
    let revivedDesc = '';
    if (msg.revivePieceType && msg.reviveSquare) {
      const adjacentSquares = getSurroundingSquares(msg.to);
      const isTargetEmpty =
        !chess.get(msg.reviveSquare as Square) &&
        !state.restrictedSquares.includes(msg.reviveSquare) &&
        state.monolithSquare !== msg.reviveSquare &&
        !(state.champions && (state.champions.w?.square === msg.reviveSquare || state.champions.b?.square === msg.reviveSquare));

      if (adjacentSquares.includes(msg.reviveSquare) && isTargetEmpty && msg.revivePieceType !== 'k') {
        chess.put({ type: msg.revivePieceType as any, color: currentTurn }, msg.reviveSquare as Square);
        revivedDesc = ` and revived a fallen ${msg.revivePieceType.toUpperCase()} on ${msg.reviveSquare}!`;
      }
    }

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed, true); // EXHAUSTED

    state.awakenedPieceSquare = msg.to;
    state.awakenedByPlayer = currentTurn;
    state.awakenedOnTurnNumber = state.turnNumber;
    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn, true);
    state.lastMovedPiece[currentTurn] = 'b';
    state.lastMovedSquare[currentTurn] = msg.to;

    state.awakeningAlert = {
      player: currentTurn,
      cardType: 'awakening_bishop',
      cardName: 'Awakening: Bishop',
      effectTitle: 'Fallen Piece Revived!',
      effectDescription: `${currentTurn === 'w' ? 'White' : 'Black'} moved Bishop to ${msg.to}${revivedDesc || ' in awakened power!'}`,
      pieceSquare: msg.to,
      timestamp: Date.now(),
    };

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'awakening',
      from: msg.from,
      to: msg.to,
      awakeningType: 'awakening_bishop',
      awakeningDetails: `Bishop to ${msg.to}${revivedDesc}`,
    };

    // Capture-Based Tactical Power-Up Refresh
    const { refreshed } = awardCaptureRefresh(state, currentTurn, earnedCapturePoints);
    let refreshDetail = '';
    if (refreshed.length > 0) {
      refreshDetail = ` | ⚡ Refreshed: ${refreshed.map((r) => `${r.name} (now x${r.newCount})`).join(', ')}`;
      for (const r of refreshed) {
        state.historyLog.unshift({
          id: `log_${Date.now()}_refresh_${Math.random()}`,
          turn: currentTurn,
          turnNumber: state.turnNumber,
          actionType: 'powerup_refresh',
          cardName: 'Tactical Power-Up Refresh',
          detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} refreshed ${r.name} (+1 stack, now x${r.newCount})!`,
          timestamp: Date.now(),
          whitePoints: calculatePiecePoints(chess, 'w', state.champions),
          blackPoints: calculatePiecePoints(chess, 'b', state.champions),
        });
      }
    }

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'awakening',
      cardName: 'Awakening: Bishop',
      detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} awoke Bishop to ${msg.to}${revivedDesc}! [Card Exhausted]${refreshDetail}`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w', state.champions),
      blackPoints: calculatePiecePoints(chess, 'b', state.champions),
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_awakening_queen') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'awakening_queen');
    if (cardIdx === -1) return;

    const piece = chess.get(msg.from as Square);
    if (!piece || piece.color !== currentTurn || piece.type !== 'q') {
      if (ws) sendToSocket(ws, { type: 'error', message: 'You must select your Queen to awaken!' });
      return;
    }

    const otherPlayer = currentTurn === 'w' ? 'b' : 'w';
    const validMoves = getLegalMovesForPiece(
      msg.from as Square,
      chess,
      state.restrictedSquares,
      state.monolithSquare,
      state.staticEffect?.stunnedSquares || [],
      state.beaconEffect?.active && state.monolithSquare ? getSurroundingSquares(state.monolithSquare) : [],
      state.champions ? state.champions[currentTurn]?.square || null : null,
      state.champions ? state.champions[otherPlayer] : null,
      state.champions,
      state.awakenedRooks || [],
      state.superPawns || []
    );

    if (!validMoves.includes(msg.to)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Invalid destination for Queen move!' });
      return;
    }

    const capturedPiece = chess.get(msg.to as Square);
    const capturedChamp = state.champions && state.champions[otherPlayer]?.square === msg.to;
    if (capturedChamp) {
      state.champions[otherPlayer] = null;
    }

    const boardWithMonolith = createBoardWithMonolith(state.fen, state.monolithSquare, currentTurn, state.champions);
    try {
      boardWithMonolith.move({ from: msg.from as Square, to: msg.to as Square });
    } catch (e) {
      return;
    }
    cleanMonolithAndChampions(boardWithMonolith, state.monolithSquare, state.champions, msg.to);
    chess = boardWithMonolith;

    // If Queen captured a standard enemy piece, place it on deploySquare converted to player's side!
    let convertedDesc = '';
    if (capturedPiece && capturedPiece.color !== currentTurn && capturedPiece.type !== 'k' && msg.deploySquare) {
      const adjacentSquares = getSurroundingSquares(msg.to);
      const isTargetEmpty =
        !chess.get(msg.deploySquare as Square) &&
        !state.restrictedSquares.includes(msg.deploySquare) &&
        state.monolithSquare !== msg.deploySquare &&
        !(state.champions && (state.champions.w?.square === msg.deploySquare || state.champions.b?.square === msg.deploySquare));

      if (adjacentSquares.includes(msg.deploySquare) && isTargetEmpty) {
        chess.put({ type: capturedPiece.type, color: currentTurn }, msg.deploySquare as Square);
        convertedDesc = ` and converted captured enemy ${capturedPiece.type.toUpperCase()} onto ${msg.deploySquare}!`;
      }
    }

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed, true); // EXHAUSTED

    state.awakenedPieceSquare = msg.to;
    state.awakenedByPlayer = currentTurn;
    state.awakenedOnTurnNumber = state.turnNumber;
    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn, true);
    state.lastMovedPiece[currentTurn] = 'q';
    state.lastMovedSquare[currentTurn] = msg.to;

    state.awakeningAlert = {
      player: currentTurn,
      cardType: 'awakening_queen',
      cardName: 'Awakening: Queen',
      effectTitle: 'Awakened Queen Strikes!',
      effectDescription: `${currentTurn === 'w' ? 'White' : 'Black'} moved Queen to ${msg.to}${convertedDesc || ' in supreme glory!'}`,
      pieceSquare: msg.to,
      timestamp: Date.now(),
    };

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'awakening',
      from: msg.from,
      to: msg.to,
      awakeningType: 'awakening_queen',
      awakeningDetails: `Queen to ${msg.to}${convertedDesc}`,
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'awakening',
      cardName: 'Awakening: Queen',
      detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} awoke Queen to ${msg.to}${convertedDesc}! [Card Exhausted]`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w'),
      blackPoints: calculatePiecePoints(chess, 'b'),
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_awakening_king') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'awakening_king');
    if (cardIdx === -1) return;

    // Verification: King MUST be in check!
    const inCheck = isKingChecked(chess, currentTurn, state.monolithSquare, state.champions);
    if (!inCheck) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Awakening: King can only be activated when your King is in Check!' });
      return;
    }

    const piece = chess.get(msg.from as Square);
    if (!piece || piece.color !== currentTurn || piece.type !== 'k') {
      if (ws) sendToSocket(ws, { type: 'error', message: 'You must select your King!' });
      return;
    }

    const otherPlayer = currentTurn === 'w' ? 'b' : 'w';
    const validQueenMoves = getQueenMovesForKing(
      msg.from as Square,
      chess,
      state.restrictedSquares,
      state.monolithSquare,
      state.champions,
      state.beaconEffect?.active && state.monolithSquare ? getSurroundingSquares(state.monolithSquare) : [],
      state.awakenedRooks || []
    );

    if (!validQueenMoves.includes(msg.to)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Invalid Queen-style move for King!' });
      return;
    }

    if (state.champions && state.champions[otherPlayer]?.square === msg.to) {
      state.champions[otherPlayer] = null;
    }

    // Move King
    chess.remove(msg.from as Square);
    chess.remove(msg.to as Square);
    chess.put({ type: 'k', color: currentTurn }, msg.to as Square);

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed, true); // EXHAUSTED

    state.awakenedPieceSquare = msg.to;
    state.awakenedByPlayer = currentTurn;
    state.awakenedOnTurnNumber = state.turnNumber;
    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn, true);
    state.lastMovedPiece[currentTurn] = 'k';
    state.lastMovedSquare[currentTurn] = msg.to;

    state.awakeningAlert = {
      player: currentTurn,
      cardType: 'awakening_king',
      cardName: 'Awakening: King',
      effectTitle: 'Ascended King Moves Like a Queen!',
      effectDescription: `${currentTurn === 'w' ? 'White' : 'Black'}'s checked King broke free with Queen-like powers and moved to ${msg.to}!`,
      pieceSquare: msg.to,
      timestamp: Date.now(),
    };

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'awakening',
      from: msg.from,
      to: msg.to,
      awakeningType: 'awakening_king',
      awakeningDetails: `Checked King moved like a Queen to ${msg.to}`,
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'awakening',
      cardName: 'Awakening: King',
      detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} awoke King! Moved like a Queen to ${msg.to}! [Card Exhausted]`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w'),
      blackPoints: calculatePiecePoints(chess, 'b'),
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_awakening_as_wild') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type.startsWith('awakening_'));
    if (cardIdx === -1) return;

    const piece = chess.get(msg.from as Square);
    if (!piece || piece.color !== currentTurn) return;

    const otherPlayer = currentTurn === 'w' ? 'b' : 'w';
    const validMoves = getLegalMovesForPiece(
      msg.from as Square,
      chess,
      state.restrictedSquares,
      state.monolithSquare,
      state.staticEffect?.stunnedSquares || [],
      state.beaconEffect?.active && state.monolithSquare ? getSurroundingSquares(state.monolithSquare) : [],
      state.champions ? state.champions[currentTurn]?.square || null : null,
      state.champions ? state.champions[otherPlayer] : null,
      state.champions,
      state.awakenedRooks || [],
      state.superPawns || []
    );

    if (!validMoves.includes(msg.to)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Invalid move for piece!' });
      return;
    }

    if (state.champions && state.champions[otherPlayer]?.square === msg.to) {
      state.champions[otherPlayer] = null;
    }

    const boardWithMonolith = createBoardWithMonolith(state.fen, state.monolithSquare, currentTurn, state.champions);
    let moveResult: any = null;
    try {
      moveResult = boardWithMonolith.move({
        from: msg.from as Square,
        to: msg.to as Square,
        promotion: msg.promotion || 'q',
      });
    } catch (e) {
      return;
    }
    if (!moveResult) return;

    cleanMonolithAndChampions(boardWithMonolith, state.monolithSquare, state.champions, msg.to);
    chess = boardWithMonolith;

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed, false); // RECYCLES TO DISCARD (not exhausted)

    // Update piece tracking: Super Pawns stay permanent until game end!
    if (!state.superPawns) state.superPawns = [];
    if (!state.superKnights) state.superKnights = [];

    const isTargetSquareSuperPawn = Boolean(state.superPawns.includes(msg.to));
    if (state.superPawns.includes(msg.from)) {
      state.superPawns = state.superPawns.filter((sq) => sq !== msg.from && sq !== msg.to);
      if (piece.type === 'p' && !moveResult?.promotion) state.superPawns.push(msg.to);
    } else if (isTargetSquareSuperPawn) {
      state.superPawns = state.superPawns.filter((sq) => sq !== msg.to);
    }

    const isTargetSquareSuperKnight = Boolean(state.superKnights.includes(msg.to));
    if (state.superKnights.includes(msg.from)) {
      state.superKnights = state.superKnights.filter((sq) => sq !== msg.from && sq !== msg.to);
      state.superKnights.push(msg.to);
    } else if (isTargetSquareSuperKnight) {
      state.superKnights = state.superKnights.filter((sq) => sq !== msg.to);
    }
    if (state.awakenedRooks?.some((r) => r.square === msg.from)) {
      state.awakenedRooks = state.awakenedRooks.map((r) =>
        r.square === msg.from ? { ...r, square: msg.to, hasCaptured: r.hasCaptured || Boolean(moveResult?.captured) } : r
      );
    }

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn, true);
    state.lastMovedPiece[currentTurn] = piece.type;
    state.lastMovedSquare[currentTurn] = msg.to;

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'move',
      from: msg.from,
      to: msg.to,
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'move',
      cardName: `${cardPlayed.name} (Played as Wild)`,
      detail: `${currentTurn === 'w' ? 'White' : 'Black'} played ${cardPlayed.name} as Wild: ${msg.from}->${msg.to} (Recycled to discard)`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w'),
      blackPoints: calculatePiecePoints(chess, 'b'),
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_champion_deploy') {
    if (state.champions && state.champions[currentTurn]) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'You already have an active Champion on the board!' });
      return;
    }
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'champion');
    if (cardIdx === -1) return;

    const otherColor = currentTurn === 'w' ? 'b' : 'w';
    const validDeploySquares = getChampionDeploySquares(
      currentTurn,
      chess,
      state.restrictedSquares,
      state.monolithSquare,
      state.champions ? state.champions[otherColor] : null
    );
    if (!validDeploySquares.includes(msg.targetSquare)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Invalid deployment square on your side of the board!' });
      return;
    }

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed);

    if (!state.champions) {
      state.champions = { w: null, b: null };
    }
    const championTypes: ('berserker' | 'musketeer' | 'summoner' | 'ninja')[] = [
      'berserker',
      'musketeer',
      'summoner',
      'ninja',
    ];
    const pickedType: 'berserker' | 'musketeer' | 'summoner' | 'ninja' =
      msg.championType && championTypes.includes(msg.championType)
        ? msg.championType
        : championTypes[Math.floor(Math.random() * championTypes.length)];

    state.champions[currentTurn] = {
      type: pickedType,
      color: currentTurn,
      square: msg.targetSquare,
    };

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn);

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'champion_deploy',
      championType: pickedType,
      championSquare: msg.targetSquare,
    };

    const champName = pickedType.charAt(0).toUpperCase() + pickedType.slice(1);
    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'champion_deploy',
      cardName: 'Champion Deployed',
      detail: `⚔️ ${currentTurn === 'w' ? 'White' : 'Black'} deployed Champion ${champName} at ${msg.targetSquare}!`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_champion_move') {
    const champ = state.champions ? state.champions[currentTurn] : null;
    if (!champ || champ.square !== msg.from) return;

    const otherColor = currentTurn === 'w' ? 'b' : 'w';
    const enemyChamp = state.champions ? state.champions[otherColor] : null;
    const legalMoves = getChampionNormalMoves(
      champ,
      chess,
      state.restrictedSquares,
      state.monolithSquare,
      enemyChamp,
      state.staticEffect?.stunnedSquares || [],
      state.beaconEffect?.protectedSquares || []
    );
    if (!legalMoves.includes(msg.to)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Illegal champion move!' });
      return;
    }

    const capturedPiece = chess.get(msg.to as Square);
    if (capturedPiece) {
      chess.remove(msg.to as Square);
    }
    let capturedEnemyChamp = false;
    if (enemyChamp && enemyChamp.square === msg.to) {
      if (state.champions) state.champions[otherColor] = null;
      capturedEnemyChamp = true;
    }

    // Beacon buff follows champion when it moves
    updateBeaconPieceSquare(state.beaconEffect, msg.from, msg.to);

    champ.square = msg.to;
    consumeCardAndRefill(state, currentTurn, null);

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn);

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'champion_move',
      from: msg.from,
      to: msg.to,
      championType: champ.type,
    };

    const champName = champ.type.charAt(0).toUpperCase() + champ.type.slice(1);

    // Capture-Based Tactical Power-Up Refresh
    let earnedCapturePoints = 0;
    if (capturedPiece) {
      earnedCapturePoints += getPieceCapturePoints(capturedPiece.type);
    }
    if (capturedEnemyChamp) {
      earnedCapturePoints += 5;
    }

    const { refreshed } = awardCaptureRefresh(state, currentTurn, earnedCapturePoints);
    let refreshDetail = '';
    if (refreshed.length > 0) {
      refreshDetail = ` | ⚡ Refreshed: ${refreshed.map((r) => `${r.name} (now x${r.newCount})`).join(', ')}`;
      for (const r of refreshed) {
        state.historyLog.unshift({
          id: `log_${Date.now()}_refresh_${Math.random()}`,
          turn: currentTurn,
          turnNumber: state.turnNumber,
          actionType: 'powerup_refresh',
          cardName: 'Tactical Power-Up Refresh',
          detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} refreshed ${r.name} (+1 stack, now x${r.newCount})!`,
          timestamp: Date.now(),
          whitePoints: calculatePiecePoints(chess, 'w', state.champions),
          blackPoints: calculatePiecePoints(chess, 'b', state.champions),
        });
      }
    }

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'champion_move',
      cardName: `${champName} Move`,
      detail: `⚔️ ${currentTurn === 'w' ? 'White' : 'Black'}'s ${champName} moved to ${msg.to}${capturedPiece ? ` (Captured ${capturedPiece.type.toUpperCase()})` : ''}${capturedEnemyChamp ? ' (Defeated enemy Champion!)' : ''}.${refreshDetail}`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w', state.champions),
      blackPoints: calculatePiecePoints(chess, 'b', state.champions),
    });

    const evalRes = evaluateGameEnd(chess, state);
    state.status = evalRes.status;
    state.winner = evalRes.winner;
    state.winReason = evalRes.winReason;
    state.whitePoints = evalRes.whitePoints;
    state.blackPoints = evalRes.blackPoints;

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_champion_ultimate') {
    const champ = state.champions ? state.champions[currentTurn] : null;
    if (!champ) return;
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'champion');
    if (cardIdx === -1) return;

    const otherColor = currentTurn === 'w' ? 'b' : 'w';
    const enemyChamp = state.champions ? state.champions[otherColor] : null;
    const champName = champ.type.charAt(0).toUpperCase() + champ.type.slice(1);
    let ultimateDetail = '';
    let earnedCapturePoints = 0;

    if (champ.type === 'berserker') {
      if (!msg.targetSquare) return;
      const validMoves = getBerserkerUltimateMoves(
        champ,
        chess,
        state.restrictedSquares,
        state.monolithSquare,
        enemyChamp,
        state.beaconEffect?.protectedSquares || []
      );
      if (!validMoves.includes(msg.targetSquare)) return;

      const capturedPiece = chess.get(msg.targetSquare as Square);
      if (capturedPiece) {
        earnedCapturePoints += getPieceCapturePoints(capturedPiece.type);
        chess.remove(msg.targetSquare as Square);
      }
      if (enemyChamp && enemyChamp.square === msg.targetSquare && state.champions) {
        earnedCapturePoints += 5;
        state.champions[otherColor] = null;
      }
      updateBeaconPieceSquare(state.beaconEffect, champ.square, msg.targetSquare);
      champ.square = msg.targetSquare;
      ultimateDetail = `Berserker swept across the board like a Queen to ${msg.targetSquare}${capturedPiece ? ` (Captured ${capturedPiece.type.toUpperCase()})` : ''}!`;
    } else if (champ.type === 'musketeer') {
      if (!msg.targetSquare) return;
      const validTargets = getMusketeerSnipeTargets(
        champ,
        chess,
        state.monolithSquare,
        enemyChamp,
        state.beaconEffect?.protectedSquares || []
      );
      if (!validTargets.includes(msg.targetSquare)) return;

      const capturedPiece = chess.get(msg.targetSquare as Square);
      if (capturedPiece) {
        earnedCapturePoints += getPieceCapturePoints(capturedPiece.type);
        chess.remove(msg.targetSquare as Square);
      }
      if (enemyChamp && enemyChamp.square === msg.targetSquare && state.champions) {
        earnedCapturePoints += 5;
        state.champions[otherColor] = null;
      }
      ultimateDetail = `Musketeer sniped target at ${msg.targetSquare} from range without moving!`;
    } else if (champ.type === 'summoner') {
      if (!msg.targetFriendlySquare || !msg.targetSquare) return;
      const friendlyPiece = chess.get(msg.targetFriendlySquare as Square);
      if (!friendlyPiece || friendlyPiece.color !== currentTurn) return;

      const validPlacement = getSummonerPlacementSquares(
        champ.square,
        currentTurn,
        chess,
        state.restrictedSquares,
        state.monolithSquare,
        enemyChamp,
        state.beaconEffect?.protectedSquares || []
      );
      if (!validPlacement.includes(msg.targetSquare)) return;

      const opponentPiece = chess.get(msg.targetSquare as Square);
      if (opponentPiece) {
        earnedCapturePoints += getPieceCapturePoints(opponentPiece.type);
        chess.remove(msg.targetSquare as Square);
      }
      if (enemyChamp && enemyChamp.square === msg.targetSquare && state.champions) {
        earnedCapturePoints += 5;
        state.champions[otherColor] = null;
      }

      chess.remove(msg.targetFriendlySquare as Square);
      chess.put(friendlyPiece, msg.targetSquare as Square);
      updateBeaconPieceSquare(state.beaconEffect, msg.targetFriendlySquare, msg.targetSquare);
      ensureEdgePawnsPromoted(chess);
      ultimateDetail = `Summoner summoned & teleported friendly ${friendlyPiece.type.toUpperCase()} from ${msg.targetFriendlySquare} beside him to ${msg.targetSquare}${opponentPiece ? ' (Captured opponent piece!)' : ''}!`;
    } else if (champ.type === 'ninja') {
      const targets = msg.targetSquares || (msg.targetSquare ? [msg.targetSquare] : []);
      if (targets.length === 0 || targets.length > 2) return;

      const validAdjacent = getNinjaAdjacentEnemyPieces(
        champ.square,
        currentTurn,
        chess,
        enemyChamp,
        state.beaconEffect?.protectedSquares || []
      );
      for (const t of targets) {
        if (!validAdjacent.includes(t)) return;
      }

      for (const t of targets) {
        const piece = chess.get(t as Square);
        if (piece) {
          earnedCapturePoints += getPieceCapturePoints(piece.type);
          chess.remove(t as Square);
        }
        if (enemyChamp && enemyChamp.square === t && state.champions) {
          earnedCapturePoints += 5;
          state.champions[otherColor] = null;
        }
      }
      ultimateDetail = `Ninja assassinated ${targets.length} surrounding enemy target(s) at [${targets.join(', ')}] without moving!`;
    }

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed);

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn);

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'champion_ultimate',
      championType: champ.type,
      ultimateDetails: ultimateDetail,
    };

    // Capture-Based Tactical Power-Up Refresh
    const { refreshed } = awardCaptureRefresh(state, currentTurn, earnedCapturePoints);
    let refreshDetail = '';
    if (refreshed.length > 0) {
      refreshDetail = ` | ⚡ Refreshed: ${refreshed.map((r) => `${r.name} (now x${r.newCount})`).join(', ')}`;
      for (const r of refreshed) {
        state.historyLog.unshift({
          id: `log_${Date.now()}_refresh_${Math.random()}`,
          turn: currentTurn,
          turnNumber: state.turnNumber,
          actionType: 'powerup_refresh',
          cardName: 'Tactical Power-Up Refresh',
          detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} refreshed ${r.name} (+1 stack, now x${r.newCount})!`,
          timestamp: Date.now(),
          whitePoints: calculatePiecePoints(chess, 'w', state.champions),
          blackPoints: calculatePiecePoints(chess, 'b', state.champions),
        });
      }
    }

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'champion_ultimate',
      cardName: `${champName} Ultimate`,
      detail: `💥 ${currentTurn === 'w' ? 'White' : 'Black'} triggered ${champName}'s Ultimate: ${ultimateDetail}${refreshDetail}`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w', state.champions),
      blackPoints: calculatePiecePoints(chess, 'b', state.champions),
    });

    const evalRes = evaluateGameEnd(chess, state);
    state.status = evalRes.status;
    state.winner = evalRes.winner;
    state.winReason = evalRes.winReason;
    state.whitePoints = evalRes.whitePoints;
    state.blackPoints = evalRes.blackPoints;

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'discard_redraw') {
    // "Instead of Passing the Turn, you can discard up to 5 cards and redraw the same number and it counts as a move."
    const toDiscardIds = Array.isArray(msg.cardIds) ? msg.cardIds.slice(0, 5) : [];
    if (toDiscardIds.length === 0) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Select at least 1 card to discard and redraw' });
      return;
    }

    const hand = state.hands[currentTurn];
    const keptHand: Card[] = [];
    const discardedCards: Card[] = [];

    for (const c of hand) {
      if (toDiscardIds.includes(c.id)) {
        discardedCards.push(c);
        // Only non-joker-wild cards go to the shared discard pile
        if (!c.isJokerWild) {
          state.discard.push(c);
        }
      } else {
        keptHand.push(c);
      }
    }

    // Redraw the exact same number of cards from deck
    const redrawCount = discardedCards.length;
    for (let i = 0; i < redrawCount; i++) {
      const drawRes = drawCard(state);
      if (drawRes.drawnCard) {
        keptHand.push(drawRes.drawnCard);
      }
      state.deck = drawRes.newDeck;
      state.discard = drawRes.newDiscard;
      if (drawRes.reshuffled) {
        state.powerUps = drawRes.powerUps;
      }
    }

    state.hands[currentTurn] = keptHand;

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(state.fen, nextTurn, false);

    const playerName = currentTurn === 'w' ? 'White' : 'Black';
    const alertMessage = `${playerName} only passed and redrew ${redrawCount} card${redrawCount > 1 ? 's' : ''} on their turn (no piece moved).`;

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'redraw',
    };

    state.passRedrawAlert = {
      player: currentTurn,
      action: 'redraw',
      count: redrawCount,
      message: alertMessage,
      timestamp: Date.now(),
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'pass',
      detail: `🎴 ${playerName} only passed and redrew ${redrawCount} card${redrawCount > 1 ? 's' : ''} on their turn (no piece moved).`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'pass_turn') {
    // "Passing is allowed if you don't have any cards to play."
    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(state.fen, nextTurn, false);

    const playerName = currentTurn === 'w' ? 'White' : 'Black';
    const alertMessage = `${playerName} passed their turn (no piece moved).`;

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'pass',
    };

    state.passRedrawAlert = {
      player: currentTurn,
      action: 'pass',
      message: alertMessage,
      timestamp: Date.now(),
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'pass',
      detail: `⏭️ ${playerName} passed their turn (no piece moved).`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'use_powerup_vision') {
    const curVision = typeof state.powerUps[currentTurn].vision === 'number' ? state.powerUps[currentTurn].vision : state.powerUps[currentTurn].vision ? 1 : 0;
    if (curVision <= 0) return;
    state.powerUps[currentTurn].vision = curVision - 1;
    state.visionActive[currentTurn] = true;

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'vision',
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'powerup',
      detail: `${currentTurn === 'w' ? 'White' : 'Black'} activated Vision power-up to peek at opponent's hand! (${state.powerUps[currentTurn].vision} stack(s) remaining)`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w', state.champions),
      blackPoints: calculatePiecePoints(chess, 'b', state.champions),
    });
    return;
  }

  if (msg.type === 'use_powerup_draw') {
    const curDraw = typeof state.powerUps[currentTurn].draw === 'number' ? state.powerUps[currentTurn].draw : state.powerUps[currentTurn].draw ? 1 : 0;
    if (curDraw <= 0) return;
    state.powerUps[currentTurn].draw = curDraw - 1;

    // Draw 2 cards
    const d1 = drawCard(state);
    if (d1.drawnCard) state.hands[currentTurn].push(d1.drawnCard);
    state.deck = d1.newDeck;
    state.discard = d1.newDiscard;

    const d2 = drawCard(state);
    if (d2.drawnCard) state.hands[currentTurn].push(d2.drawnCard);
    state.deck = d2.newDeck;
    state.discard = d2.newDiscard;

    // Discard the chosen 2 cards
    const [discard1, discard2] = msg.discardCardIds;
    const hand = state.hands[currentTurn];
    const keptHand: Card[] = [];
    for (const c of hand) {
      if (c.id === discard1 || c.id === discard2) {
        state.discard.push(c);
      } else {
        keptHand.push(c);
      }
    }
    state.hands[currentTurn] = keptHand;

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'draw_powerup',
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'powerup',
      detail: `${currentTurn === 'w' ? 'White' : 'Black'} used Draw power-up (drew 2, discarded 2). (${state.powerUps[currentTurn].draw} stack(s) remaining)`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w', state.champions),
      blackPoints: calculatePiecePoints(chess, 'b', state.champions),
    });
    return;
  }

  if (msg.type === 'play_monolith') {
    const isPowerUp = msg.isPowerUp || false;
    let cardPlayed: Card | null = null;

    // Check monolith rule: cannot be moved if placed or moved on previous turn
    if (
      state.monolithSquare &&
      state.monolithLastMovedTurn !== null &&
      state.monolithLastMovedTurn === state.turnNumber - 1
    ) {
      if (ws) {
        sendToSocket(ws, {
          type: 'error',
          message: 'Monolith cannot be moved: It was placed or moved on the previous turn!',
        });
      }
      return;
    }

    if (isPowerUp) {
      const curMonolith = typeof state.powerUps[currentTurn].monolith === 'number' ? state.powerUps[currentTurn].monolith : state.powerUps[currentTurn].monolith ? 1 : 0;
      if (curMonolith <= 0) return;
      state.powerUps[currentTurn].monolith = curMonolith - 1;
    } else {
      if (!msg.cardId) return;
      const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'monolith');
      if (cardIdx === -1) return;
      cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
      consumeCardAndRefill(state, currentTurn, cardPlayed);
    }

    // Place or move monolith
    const target = msg.targetSquare;
    if (state.monolithSquare && target === state.monolithSquare) {
      if (ws) {
        sendToSocket(ws, {
          type: 'error',
          message: 'Cannot move the Monolith to the same square where it is currently standing!',
        });
      }
      return;
    }
    if (isSquareBlocked(target, state.restrictedSquares, null) || chess.get(target as Square)) {
      return; // Cannot place on occupied or restricted square
    }

    const prevMonolith = state.monolithSquare;
    state.monolithSquare = target;
    state.monolithLastMovedTurn = state.turnNumber;

    // If Monolith changes position, Beacon invulnerability field moves to the new position
    // (pieces left at old position lose invulnerability)
    if (state.beaconEffect?.active) {
      state.beaconEffect.protectedSquares = getSurroundingSquares(target);
    }
    if (state.staticEffect?.active) {
      state.staticEffect.stunnedSquares = getSurroundingSquares(target);
    }

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'monolith',
      monolithFrom: prevMonolith,
      monolithSquare: target,
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'monolith',
      cardName: cardPlayed ? 'Monolith Card' : 'Monolith Power-up',
      detail: `${currentTurn === 'w' ? 'White' : 'Black'} ${prevMonolith ? `moved Monolith from ${prevMonolith} to ${target}` : `summoned Monolith to ${target}`}.`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(state.fen, nextTurn);
    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_restrict') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'restrict');
    if (cardIdx === -1) return;

    const target = msg.targetSquare;
    // The Restrict card can only choose empty squares (no piece, no monolith, not already restricted)
    if (
      !target ||
      chess.get(target as Square) ||
      state.monolithSquare === target ||
      state.restrictedSquares.includes(target)
    ) {
      return;
    }

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed);

    if (!state.restrictedSquares.includes(target)) {
      state.restrictedSquares.push(target);
    }

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'restrict',
      restrictedSquare: target,
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'restrict',
      cardName: 'Restrict Card',
      detail: `${currentTurn === 'w' ? 'White' : 'Black'} marked square ${target} as permanently restricted!`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(state.fen, nextTurn);
    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_switch') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'switch');
    if (cardIdx === -1) return;
    if (msg.square1 === msg.square2) return;

    const p1 = chess.get(msg.square1 as Square);
    const p2 = chess.get(msg.square2 as Square);
    const isChamp1 = Boolean(state.champions && state.champions[currentTurn]?.square === msg.square1);
    const isChamp2 = Boolean(state.champions && state.champions[currentTurn]?.square === msg.square2);

    if ((!p1 && !isChamp1) || (!p2 && !isChamp2)) return;
    if (p1 && p1.color !== currentTurn) return;
    if (p2 && p2.color !== currentTurn) return;

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed);

    let name1 = '';
    let name2 = '';

    if (isChamp1 && !isChamp2) {
      name1 = `Champion ${state.champions![currentTurn]!.type.toUpperCase()}`;
      name2 = p2!.type.toUpperCase();
      chess.remove(msg.square2 as Square);
      chess.put(p2!, msg.square1 as Square);
      state.champions![currentTurn]!.square = msg.square2;
      state.lastMovedPiece[currentTurn] = p2!.type;
      state.lastMovedSquare[currentTurn] = msg.square1;
    } else if (!isChamp1 && isChamp2) {
      name1 = p1!.type.toUpperCase();
      name2 = `Champion ${state.champions![currentTurn]!.type.toUpperCase()}`;
      chess.remove(msg.square1 as Square);
      chess.put(p1!, msg.square2 as Square);
      state.champions![currentTurn]!.square = msg.square1;
      state.lastMovedPiece[currentTurn] = p1!.type;
      state.lastMovedSquare[currentTurn] = msg.square2;
    } else if (isChamp1 && isChamp2) {
      name1 = `Champion ${state.champions![currentTurn]!.type.toUpperCase()}`;
      name2 = name1;
      state.champions![currentTurn]!.square = msg.square2;
    } else {
      name1 = p1!.type.toUpperCase();
      name2 = p2!.type.toUpperCase();
      chess.remove(msg.square1 as Square);
      chess.remove(msg.square2 as Square);
      chess.put(p2!, msg.square1 as Square);
      chess.put(p1!, msg.square2 as Square);
      state.lastMovedPiece[currentTurn] = p1!.type;
      state.lastMovedSquare[currentTurn] = msg.square2;
    }

    ensureEdgePawnsPromoted(chess);

    if (state.superPawns && state.superPawns.length > 0) {
      const sp1 = state.superPawns.includes(msg.square1);
      const sp2 = state.superPawns.includes(msg.square2);
      if (sp1 && !sp2) {
        state.superPawns = state.superPawns.filter((s) => s !== msg.square1).concat(msg.square2);
      } else if (!sp1 && sp2) {
        state.superPawns = state.superPawns.filter((s) => s !== msg.square2).concat(msg.square1);
      }
    }

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn);

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'switch',
      swappedSquares: [msg.square1, msg.square2],
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'switch',
      cardName: 'Switch Card',
      detail: `${currentTurn === 'w' ? 'White' : 'Black'} swapped ${name1} at ${msg.square1} and ${name2} at ${msg.square2}.`,
      timestamp: Date.now(),
      whitePoints: state.whitePoints,
      blackPoints: state.blackPoints,
    });

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_push') {
    const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId && c.type === 'push');
    if (cardIdx === -1) return;

    const piece = chess.get(msg.from as Square);
    if (!piece || piece.color !== currentTurn) return;

    // Champions block Push card movement
    const validPushMoves = getPushMoves(
      msg.from,
      chess,
      state.restrictedSquares,
      state.monolithSquare,
      state.champions
    );
    if (!validPushMoves.includes(msg.to)) return;

    const cardPlayed = state.hands[currentTurn].splice(cardIdx, 1)[0];
    consumeCardAndRefill(state, currentTurn, cardPlayed);

    // Move piece on chess board (no capture, simple move)
    chess.remove(msg.from as Square);

    // Pawn Promotion check: only when reaching opponent's back rank
    const targetRank = msg.to[1];
    let isPromoted = false;
    let placedPiece = piece;
    if (piece.type === 'p') {
      const reachesOpponentBackline =
        (piece.color === 'w' && targetRank === '8') ||
        (piece.color === 'b' && targetRank === '1');
      if (reachesOpponentBackline) {
        placedPiece = { type: 'q', color: piece.color };
        isPromoted = true;
      }
    }

    chess.put(placedPiece, msg.to as Square);
    ensureEdgePawnsPromoted(chess);

    if (state.superPawns?.includes(msg.from)) {
      state.superPawns = state.superPawns.filter((s) => s !== msg.from);
      if (!isPromoted) {
        state.superPawns.push(msg.to);
      }
    }

    const nextTurn = currentTurn === 'w' ? 'b' : 'w';
    state.fen = syncFenTurn(chess.fen(), nextTurn);
    state.lastMovedPiece[currentTurn] = placedPiece.type;
    state.lastMovedSquare[currentTurn] = msg.to;

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'push',
      from: msg.from,
      to: msg.to,
    };

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: 'push',
      cardName: 'Push Card',
      detail: `${currentTurn === 'w' ? 'White' : 'Black'} pushed ${piece.type.toUpperCase()} from ${msg.from} to ${msg.to}${isPromoted ? ' (Promoted to Queen!)' : ''}.`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w'),
      blackPoints: calculatePiecePoints(chess, 'b'),
    });

    // Check game end conditions
    const evalRes = evaluateGameEnd(chess, state);
    state.status = evalRes.status;
    state.winner = evalRes.winner;
    state.winReason = evalRes.winReason;
    state.whitePoints = evalRes.whitePoints;
    state.blackPoints = evalRes.blackPoints;

    advanceTurnState(state, currentTurn, nextTurn);
    return;
  }

  if (msg.type === 'play_king_move' || msg.type === 'play_card_move' || msg.type === 'use_powerup_wild_move') {
    const piece = chess.get(msg.from as Square);
    if (!piece || piece.color !== currentTurn) return;

    const isKingMove = piece.type === 'k';
    let cardPlayed: Card | null = null;
    let cardIdxToRemove = -1;

    if (msg.type === 'play_king_move') {
      if (!isKingMove) return;
      // King move and castling do not require a card
    } else if (msg.type === 'use_powerup_wild_move') {
      const curWild = typeof state.powerUps[currentTurn].wild === 'number' ? state.powerUps[currentTurn].wild : state.powerUps[currentTurn].wild ? 1 : 0;
      if (curWild <= 0) return;
      state.powerUps[currentTurn].wild = curWild - 1;
    } else {
      // Standard card move
      if (isKingMove) {
        // King can move without card, or with Wild card
      } else {
        if (!msg.cardId) return;
        const cardIdx = state.hands[currentTurn].findIndex((c) => c.id === msg.cardId);
        if (cardIdx === -1) return;

        const candidateCard = state.hands[currentTurn][cardIdx];
        if (!isCardPermittedForPiece(candidateCard, piece.type, state.lastMovedPiece[currentTurn])) {
          return;
        }

        cardPlayed = candidateCard;
        cardIdxToRemove = cardIdx;
      }
    }

    // Check if piece is stunned by Static
    if (state.staticEffect?.stunnedSquares?.includes(msg.from)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'This piece is stunned by Static and cannot move!' });
      return;
    }

    // Check if target is restricted or monolith (Monolith cannot be captured)
    if (state.restrictedSquares.includes(msg.to) || state.monolithSquare === msg.to) {
      return;
    }

    // Castling check: Impossible to castle if either the king or the rook lands on the restricted tile
    if (piece.type === 'k' && isCastlingBlockedByRestrictedSquares(msg.from, msg.to, state.restrictedSquares)) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Cannot castle: King or Rook lands on a restricted tile!' });
      return;
    }

    // Cannot land on friendly champion
    if (state.champions && state.champions[currentTurn]?.square === msg.to) {
      return;
    }

    // Cannot capture piece protected by Beacon (only surrounding the Monolith receives the invulnerability buff)
    const isEnPassantAttempt = piece.type === 'p' && msg.from[0] !== msg.to[0] && !chess.get(msg.to as Square);
    const capturedTargetSquare = isEnPassantAttempt ? `${msg.to[0]}${msg.from[1]}` : msg.to;
    const isTargetBeaconProtected = Boolean(
      state.beaconEffect?.active &&
      state.monolithSquare &&
      getSurroundingSquares(state.monolithSquare).includes(capturedTargetSquare)
    );

    if (isTargetBeaconProtected && (isEnPassantAttempt || !!chess.get(msg.to as Square))) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Target piece is in the Beacon field surrounding the Monolith and is invulnerable!' });
      return;
    }

    // Cannot capture piece protected by Awakened Rook invulnerable shield
    const isTargetAwakenedRook = Boolean(
      state.awakenedRooks?.some(
        (ar) => ar.square === capturedTargetSquare && ar.color !== currentTurn && ar.turnsRemaining > 0
      )
    );
    if (isTargetAwakenedRook && (isEnPassantAttempt || !!chess.get(msg.to as Square))) {
      if (ws) sendToSocket(ws, { type: 'error', message: 'Target Rook is protected by an Invulnerable Shield!' });
      return;
    }

    // If destination has enemy champion, verify not protected by Beacon or shield
    const otherPlayerColor = currentTurn === 'w' ? 'b' : 'w';
    let defeatedEnemyChamp = false;
    if (state.champions && state.champions[otherPlayerColor]?.square === msg.to) {
      if (isTargetBeaconProtected || isTargetAwakenedRook) {
        if (ws) sendToSocket(ws, { type: 'error', message: 'Target Champion is protected by an invulnerable field!' });
        return;
      }
      state.champions[otherPlayerColor] = null;
      defeatedEnemyChamp = true;
    }

    // If moving piece is an Awakened Rook that already captured, it CANNOT capture again while awakened
    const movingAwakenedRook = state.awakenedRooks?.find((r) => r.square === msg.from && r.turnsRemaining > 0);
    if (movingAwakenedRook?.hasCaptured) {
      if (isEnPassantAttempt || !!chess.get(msg.to as Square) || defeatedEnemyChamp) {
        if (ws) sendToSocket(ws, { type: 'error', message: 'This Awakened Rook has already captured once and can no longer capture while awakened!' });
        return;
      }
    }

    let moveResult: any = null;

    // Check if this is a Super Pawn forward capture!
    const isSuperPawnForwardCapture =
      piece.type === 'p' &&
      Boolean(state.superPawns?.includes(msg.from)) &&
      msg.from[0] === msg.to[0] &&
      (currentTurn === 'w' ? parseInt(msg.to[1], 10) - parseInt(msg.from[1], 10) === 1 : parseInt(msg.from[1], 10) - parseInt(msg.to[1], 10) === 1) &&
      (Boolean(chess.get(msg.to as Square)) || defeatedEnemyChamp);

    if (isSuperPawnForwardCapture) {
      chess.remove(msg.from as Square);
      chess.remove(msg.to as Square);
      const promo = (msg as any).promotion || 'q';
      const isPromotion = (currentTurn === 'w' && msg.to[1] === '8') || (currentTurn === 'b' && msg.to[1] === '1');
      chess.put({ type: isPromotion ? (promo as any) : 'p', color: currentTurn }, msg.to as Square);
      moveResult = { san: `${msg.from}x${msg.to}`, captured: 'p', promotion: isPromotion ? promo : undefined };
    } else {
      // Use board with monolith/champions so sliding enemy threats blocked by obstacles do not cause false check/rejection
      const boardWithMonolith = createBoardWithMonolith(state.fen, state.monolithSquare, currentTurn, state.champions);
      // If capturing enemy champion, add a temporary enemy piece on msg.to so chess.js validates capture for any piece
      if (defeatedEnemyChamp) {
        boardWithMonolith.put({ type: 'p', color: otherPlayerColor }, msg.to as Square);
        ((boardWithMonolith as any)._placeholderSquares = (boardWithMonolith as any)._placeholderSquares || []).push(msg.to);
      }

      try {
        moveResult = boardWithMonolith.move({
          from: msg.from as Square,
          to: msg.to as Square,
          promotion: (msg as any).promotion || 'q',
        });
      } catch (e) {
        return;
      }

      if (!moveResult) return;

      // Cleanly remove the temporary monolith & champion placeholders while preserving en-passant square
      cleanMonolithAndChampions(boardWithMonolith, state.monolithSquare, state.champions, msg.to);
      chess = boardWithMonolith;
    }

    if (!moveResult) return;

    // Check if moving piece was a Super Knight doing a double move via normal move message
    const isMovingSuperKnight = Boolean(state.superKnights?.includes(msg.from));
    let usedSuperKnightDoubleMove = false;
    let finalMoveSquare = msg.to;
    let didSecondCapture = false;
    let secondCapturedPieceType: string | null = null;

    if (isMovingSuperKnight && (msg as any).secondTo) {
      const secondToSq = (msg as any).secondTo as Square;
      const validMoves2 = getLegalMovesForPiece(
        msg.to as Square,
        chess,
        state.restrictedSquares,
        state.monolithSquare,
        state.staticEffect?.stunnedSquares || [],
        state.beaconEffect?.active && state.monolithSquare ? getSurroundingSquares(state.monolithSquare) : [],
        state.champions ? state.champions[currentTurn]?.square || null : null,
        state.champions ? state.champions[otherPlayerColor] : null,
        state.champions,
        state.awakenedRooks || [],
        state.superPawns || []
      );
      const move1Captured = Boolean(moveResult?.captured) || defeatedEnemyChamp;
      const pieceAtSecond = chess.get(secondToSq);
      const isSecondChampCapture = Boolean(state.champions && state.champions[otherPlayerColor]?.square === secondToSq);
      const isSecondCapture = Boolean(pieceAtSecond) || isSecondChampCapture;
      if (!(move1Captured && isSecondCapture) && validMoves2.includes(secondToSq)) {
        if (isSecondChampCapture) {
          state.champions![otherPlayerColor] = null;
          didSecondCapture = true;
          secondCapturedPieceType = 'champion';
        } else if (pieceAtSecond) {
          didSecondCapture = true;
          secondCapturedPieceType = pieceAtSecond.type;
        }
        try {
          chess.move({ from: msg.to as Square, to: secondToSq });
          finalMoveSquare = secondToSq;
          usedSuperKnightDoubleMove = true;
        } catch (e) {
          // ignore move 2 failure
        }
      }
    }

    // Update piece tracking for Super Pawns, Super Knights, and Awakened Rooks
    if (!state.superPawns) state.superPawns = [];
    if (!state.superKnights) state.superKnights = [];

    const isTargetSuperPawn = Boolean(state.superPawns.includes(msg.to) || (usedSuperKnightDoubleMove && state.superPawns.includes(finalMoveSquare)));
    if (state.superPawns.includes(msg.from)) {
      state.superPawns = state.superPawns.filter((sq) => sq !== msg.from && sq !== msg.to && sq !== finalMoveSquare);
      if (piece.type === 'p' && !moveResult?.promotion) {
        state.superPawns.push(msg.to);
      }
    } else if (isTargetSuperPawn) {
      state.superPawns = state.superPawns.filter((sq) => sq !== msg.to && sq !== finalMoveSquare);
    }

    const isTargetSuperKnight = Boolean(state.superKnights.includes(msg.to) || (usedSuperKnightDoubleMove && state.superKnights.includes(finalMoveSquare)));
    if (state.superKnights.includes(msg.from)) {
      state.superKnights = state.superKnights.filter((sq) => sq !== msg.from && sq !== msg.to && sq !== finalMoveSquare);
      if (!usedSuperKnightDoubleMove) {
        state.superKnights.push(msg.to);
      }
      // If usedSuperKnightDoubleMove, reverted to normal knight!
    } else if (isTargetSuperKnight) {
      state.superKnights = state.superKnights.filter((sq) => sq !== msg.to && sq !== finalMoveSquare);
    }

    if (state.awakenedRooks?.some((r) => r.square === msg.from)) {
      state.awakenedRooks = state.awakenedRooks.map((r) => {
        if (r.square === msg.from) {
          const didCapture = Boolean(moveResult?.captured) || defeatedEnemyChamp;
          return {
            ...r,
            square: finalMoveSquare,
            hasCaptured: r.hasCaptured || didCapture,
          };
        }
        return r;
      });
    }

    if (!moveResult) return;

    // Successfully moved! Now consume card if one was used
    if (cardIdxToRemove !== -1) {
      state.hands[currentTurn].splice(cardIdxToRemove, 1);
      consumeCardAndRefill(state, currentTurn, cardPlayed);
    }

    ensureEdgePawnsPromoted(chess);
    const nextTurn = currentTurn === 'w' ? 'b' : 'w';

    // Track pawn double-push to allow standard En Passant on next turn
    const isPawnDoublePush = piece.type === 'p' && Math.abs(parseInt(msg.from[1], 10) - parseInt(msg.to[1], 10)) === 2;
    const epTargetSquare = isPawnDoublePush
      ? `${msg.from[0]}${(parseInt(msg.from[1], 10) + parseInt(msg.to[1], 10)) / 2}`
      : null;

    let newFen = syncFenTurn(chess.fen(), nextTurn, true);
    if (epTargetSquare) {
      const fenTokens = newFen.split(' ');
      fenTokens[3] = epTargetSquare;
      newFen = fenTokens.join(' ');
    }
    state.fen = newFen;

    state.lastMovedPiece[currentTurn] = piece.type;
    state.lastMovedSquare[currentTurn] = finalMoveSquare;
    state.passRedrawAlert = null; // Clear previous pass & redraw notice once a move is made

    state.lastMoveDetails = {
      player: currentTurn,
      actionType: 'move',
      from: msg.from,
      to: finalMoveSquare,
    };

    const actionDesc = cardPlayed
      ? `${cardPlayed.name} Card`
      : isKingMove
      ? 'King Move'
      : 'Wild Power-Up';

    const isEpCaptured = Boolean(moveResult?.flags?.includes('e'));

    // Capture-Based Tactical Power-Up Refresh
    let earnedCapturePoints = 0;
    if (moveResult?.captured) {
      earnedCapturePoints += getPieceCapturePoints(moveResult.captured);
    }
    if (defeatedEnemyChamp) {
      earnedCapturePoints += 5;
    }
    if (usedSuperKnightDoubleMove && didSecondCapture) {
      if (secondCapturedPieceType === 'champion') {
        earnedCapturePoints += 5;
      } else if (secondCapturedPieceType) {
        earnedCapturePoints += getPieceCapturePoints(secondCapturedPieceType);
      }
    }

    const { refreshed } = awardCaptureRefresh(state, currentTurn, earnedCapturePoints);
    let refreshDetail = '';
    if (refreshed.length > 0) {
      refreshDetail = ` | ⚡ Refreshed: ${refreshed.map((r) => `${r.name} (now x${r.newCount})`).join(', ')}`;
      for (const r of refreshed) {
        state.historyLog.unshift({
          id: `log_${Date.now()}_refresh_${Math.random()}`,
          turn: currentTurn,
          turnNumber: state.turnNumber,
          actionType: 'powerup_refresh',
          cardName: 'Tactical Power-Up Refresh',
          detail: `⚡ ${currentTurn === 'w' ? 'White' : 'Black'} refreshed ${r.name} (+1 stack, now x${r.newCount})!`,
          timestamp: Date.now(),
          whitePoints: calculatePiecePoints(chess, 'w', state.champions),
          blackPoints: calculatePiecePoints(chess, 'b', state.champions),
        });
      }
    }

    state.historyLog.unshift({
      id: `log_${Date.now()}_${Math.random()}`,
      turn: currentTurn,
      turnNumber: state.turnNumber,
      actionType: isKingMove ? 'king_move' : 'move',
      cardName: actionDesc,
      detail: `${currentTurn === 'w' ? 'White' : 'Black'} played ${actionDesc}: ${moveResult.san || `${msg.from}->${msg.to}`}${isEpCaptured ? ' (En Passant)' : ''}${defeatedEnemyChamp ? ' (Defeated enemy Champion!)' : ''}${refreshDetail}`,
      timestamp: Date.now(),
      whitePoints: calculatePiecePoints(chess, 'w', state.champions),
      blackPoints: calculatePiecePoints(chess, 'b', state.champions),
      isCheck: isKingChecked(chess, nextTurn, state.monolithSquare, state.champions),
    });

    // Evaluate Win Condition:
    const evalRes = evaluateGameEnd(chess, state);
    state.status = evalRes.status;
    state.winner = evalRes.winner;
    state.winReason = evalRes.winReason;
    state.whitePoints = evalRes.whitePoints;
    state.blackPoints = evalRes.blackPoints;

    advanceTurnState(state, currentTurn, nextTurn);
  }
}

// Background 1-second server tick to manage active chess clocks
setInterval(() => {
  const now = Date.now();
  rooms.forEach((room, roomId) => {
    const state = room.gameState;
    if (state.status === 'playing' && state.clock && state.clock.enabled && state.clock.isRunning && state.clock.lastTick) {
      const turn = state.turn;
      const elapsed = (now - state.clock.lastTick) / 1000;
      if (state.clock[turn] - elapsed <= 0) {
        state.clock[turn] = 0;
        state.status = 'timeout';
        const winner = turn === 'w' ? 'b' : 'w';
        state.winner = winner;
        state.winReason = `Time Expired: ${turn === 'w' ? 'White' : 'Black'} ran out of time! ${winner === 'w' ? 'White' : 'Black'} wins.`;
        broadcastRoom(roomId);
      }
    }
  });
}, 1000);

// Vite middleware & Static serving
async function startServer() {
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`Card Chess server running on http://0.0.0.0:${PORT}`);
  });
}

startServer();
