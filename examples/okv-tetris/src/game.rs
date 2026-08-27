pub const WIDTH: usize = 10;
pub const HEIGHT: usize = 16;
const PIECE_SEQUENCE: [PieceKind; 7] = [
    PieceKind::T,
    PieceKind::I,
    PieceKind::O,
    PieceKind::L,
    PieceKind::S,
    PieceKind::J,
    PieceKind::Z,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PieceKind {
    I = 1,
    O = 2,
    T = 3,
    L = 4,
    S = 5,
    J = 6,
    Z = 7,
}

impl PieceKind {
    #[must_use]
    pub const fn glyph(self) -> char {
        match self {
            Self::I => 'I',
            Self::O => 'O',
            Self::T => 'T',
            Self::L => 'L',
            Self::S => 'S',
            Self::J => 'J',
            Self::Z => 'Z',
        }
    }

    fn decode(value: u8) -> Result<Self, String> {
        match value {
            1 => Ok(Self::I),
            2 => Ok(Self::O),
            3 => Ok(Self::T),
            4 => Ok(Self::L),
            5 => Ok(Self::S),
            6 => Ok(Self::J),
            7 => Ok(Self::Z),
            _ => Err(format!("unknown piece kind {value}")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivePiece {
    pub kind: PieceKind,
    pub rotation: u8,
    pub x: i8,
    pub y: i8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Left,
    Right,
    Rotate,
    Tick,
    Drop,
    Reset,
}

impl Action {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Left => "move-left",
            Self::Right => "move-right",
            Self::Rotate => "rotate",
            Self::Tick => "tick",
            Self::Drop => "hard-drop",
            Self::Reset => "reset",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GameState {
    locked: [[u8; WIDTH]; HEIGHT],
    pub active: ActivePiece,
    pub next_piece: usize,
    pub score: u64,
    pub lines: u64,
    pub ticks: u64,
    pub game_over: bool,
}

impl Default for GameState {
    fn default() -> Self {
        let mut state = Self {
            locked: [[0; WIDTH]; HEIGHT],
            active: ActivePiece {
                kind: PIECE_SEQUENCE[0],
                rotation: 0,
                x: 3,
                y: 0,
            },
            next_piece: 1,
            score: 0,
            lines: 0,
            ticks: 0,
            game_over: false,
        };
        state.game_over = state.collides(state.active);
        state
    }
}

impl GameState {
    #[must_use]
    pub fn apply(&self, action: Action) -> Self {
        if action == Action::Reset {
            return Self::default();
        }
        if self.game_over {
            return self.clone();
        }
        let mut next = self.clone();
        match action {
            Action::Left => next.try_move(-1, 0),
            Action::Right => next.try_move(1, 0),
            Action::Rotate => next.try_rotate(),
            Action::Tick => next.advance(),
            Action::Drop => {
                while !next.collides(ActivePiece {
                    y: next.active.y + 1,
                    ..next.active
                }) {
                    next.active.y += 1;
                    next.score += 1;
                }
                next.lock_active();
            }
            Action::Reset => unreachable!(),
        }
        next.ticks += 1;
        next
    }

    #[must_use]
    pub fn visible_board(&self) -> [[u8; WIDTH]; HEIGHT] {
        let mut board = self.locked;
        if !self.game_over {
            for (x, y) in piece_cells(self.active) {
                if let Some((x, y)) = board_position(x, y) {
                    board[y][x] = self.active.kind as u8;
                }
            }
        }
        board
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(208);
        bytes.extend_from_slice(b"OKVTRIS0");
        bytes.extend_from_slice(&self.score.to_be_bytes());
        bytes.extend_from_slice(&self.lines.to_be_bytes());
        bytes.extend_from_slice(&self.ticks.to_be_bytes());
        bytes.extend_from_slice(&(self.next_piece as u64).to_be_bytes());
        bytes.push(u8::from(self.game_over));
        bytes.push(self.active.kind as u8);
        bytes.push(self.active.rotation);
        bytes.push(self.active.x.to_be_bytes()[0]);
        bytes.push(self.active.y.to_be_bytes()[0]);
        for row in self.locked {
            bytes.extend_from_slice(&row);
        }
        bytes
    }

    #[must_use]
    pub fn fingerprint(&self) -> String {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in self.encode() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("{hash:016x}")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        const HEADER: usize = 8 + 8 * 4 + 5;
        if bytes.len() != HEADER + WIDTH * HEIGHT || &bytes[..8] != b"OKVTRIS0" {
            return Err("unsupported Tetris state encoding".to_owned());
        }
        let mut cursor = 8;
        let score = take_u64(bytes, &mut cursor);
        let lines = take_u64(bytes, &mut cursor);
        let ticks = take_u64(bytes, &mut cursor);
        let next_piece = usize::try_from(take_u64(bytes, &mut cursor))
            .map_err(|_| "next-piece cursor does not fit this target".to_owned())?;
        let game_over = bytes[cursor] != 0;
        cursor += 1;
        let kind = PieceKind::decode(bytes[cursor])?;
        cursor += 1;
        let rotation = bytes[cursor];
        cursor += 1;
        let x = i8::from_be_bytes([bytes[cursor]]);
        cursor += 1;
        let y = i8::from_be_bytes([bytes[cursor]]);
        cursor += 1;
        let mut locked = [[0_u8; WIDTH]; HEIGHT];
        for row in &mut locked {
            row.copy_from_slice(&bytes[cursor..cursor + WIDTH]);
            cursor += WIDTH;
        }
        Ok(Self {
            locked,
            active: ActivePiece {
                kind,
                rotation,
                x,
                y,
            },
            next_piece,
            score,
            lines,
            ticks,
            game_over,
        })
    }

    fn try_move(&mut self, dx: i8, dy: i8) {
        let candidate = ActivePiece {
            x: self.active.x + dx,
            y: self.active.y + dy,
            ..self.active
        };
        if !self.collides(candidate) {
            self.active = candidate;
        }
    }

    fn try_rotate(&mut self) {
        let candidate = ActivePiece {
            rotation: (self.active.rotation + 1) % 4,
            ..self.active
        };
        if !self.collides(candidate) {
            self.active = candidate;
        }
    }

    fn advance(&mut self) {
        let candidate = ActivePiece {
            y: self.active.y + 1,
            ..self.active
        };
        if self.collides(candidate) {
            self.lock_active();
        } else {
            self.active = candidate;
        }
    }

    fn lock_active(&mut self) {
        for (x, y) in piece_cells(self.active) {
            if y < 0 {
                self.game_over = true;
                return;
            }
            if let Some((x, y)) = board_position(x, y) {
                self.locked[y][x] = self.active.kind as u8;
            }
        }
        let cleared = self.clear_lines();
        self.lines += cleared;
        self.score += match cleared {
            1 => 100,
            2 => 300,
            3 => 500,
            4 => 800,
            _ => 0,
        };
        self.spawn();
    }

    fn clear_lines(&mut self) -> u64 {
        let mut cleared = 0;
        let mut write = HEIGHT;
        for read in (0..HEIGHT).rev() {
            if self.locked[read].iter().all(|cell| *cell != 0) {
                cleared += 1;
            } else {
                write -= 1;
                self.locked[write] = self.locked[read];
            }
        }
        for row in &mut self.locked[..write] {
            *row = [0; WIDTH];
        }
        cleared
    }

    fn spawn(&mut self) {
        self.active = ActivePiece {
            kind: PIECE_SEQUENCE[self.next_piece % PIECE_SEQUENCE.len()],
            rotation: 0,
            x: 3,
            y: 0,
        };
        self.next_piece += 1;
        self.game_over = self.collides(self.active);
    }

    fn collides(&self, piece: ActivePiece) -> bool {
        piece_cells(piece).into_iter().any(|(x, y)| {
            let Ok(x) = usize::try_from(x) else {
                return true;
            };
            if x >= WIDTH {
                return true;
            }
            let Ok(y) = usize::try_from(y) else {
                return false;
            };
            y >= HEIGHT || self.locked[y][x] != 0
        })
    }
}

fn board_position(x: i8, y: i8) -> Option<(usize, usize)> {
    let x = usize::try_from(x).ok()?;
    let y = usize::try_from(y).ok()?;
    (x < WIDTH && y < HEIGHT).then_some((x, y))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(&bytes[*cursor..*cursor + 8]);
    *cursor += 8;
    u64::from_be_bytes(value)
}

fn piece_cells(piece: ActivePiece) -> [(i8, i8); 4] {
    let relative = match (piece.kind, piece.rotation % 4) {
        (PieceKind::I, 0 | 2) => [(0, 1), (1, 1), (2, 1), (3, 1)],
        (PieceKind::I, 1 | 3) => [(2, 0), (2, 1), (2, 2), (2, 3)],
        (PieceKind::O, _) => [(1, 0), (2, 0), (1, 1), (2, 1)],
        (PieceKind::T, 0) => [(1, 0), (0, 1), (1, 1), (2, 1)],
        (PieceKind::T, 1) => [(1, 0), (1, 1), (2, 1), (1, 2)],
        (PieceKind::T, 2) => [(0, 1), (1, 1), (2, 1), (1, 2)],
        (PieceKind::T, 3) => [(1, 0), (0, 1), (1, 1), (1, 2)],
        (PieceKind::L, 0) => [(0, 0), (0, 1), (1, 1), (2, 1)],
        (PieceKind::L, 1) => [(1, 0), (2, 0), (1, 1), (1, 2)],
        (PieceKind::L, 2) => [(0, 1), (1, 1), (2, 1), (2, 2)],
        (PieceKind::L, 3) => [(1, 0), (1, 1), (0, 2), (1, 2)],
        (PieceKind::J, 0) => [(2, 0), (0, 1), (1, 1), (2, 1)],
        (PieceKind::J, 1) => [(1, 0), (1, 1), (1, 2), (2, 2)],
        (PieceKind::J, 2) => [(0, 1), (1, 1), (2, 1), (0, 2)],
        (PieceKind::J, 3) => [(0, 0), (1, 0), (1, 1), (1, 2)],
        (PieceKind::S, 0 | 2) => [(1, 0), (2, 0), (0, 1), (1, 1)],
        (PieceKind::S, 1 | 3) => [(1, 0), (1, 1), (2, 1), (2, 2)],
        (PieceKind::Z, 0 | 2) => [(0, 0), (1, 0), (1, 1), (2, 1)],
        (PieceKind::Z, 1 | 3) => [(2, 0), (1, 1), (2, 1), (1, 2)],
        _ => unreachable!(),
    };
    relative.map(|(x, y)| (x + piece.x, y + piece.y))
}
