pub const BOARD_LEN: usize = 64;
const STATE_MAGIC: &[u8; 8] = b"OKVCHS00";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Color {
    White,
    Black,
}

impl Color {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::White => "white",
            Self::Black => "black",
        }
    }

    const fn other(self) -> Self {
        match self {
            Self::White => Self::Black,
            Self::Black => Self::White,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChessState {
    pub board: [u8; BOARD_LEN],
    pub turn: Color,
    pub ply: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppliedMove {
    pub from: usize,
    pub to: usize,
    pub piece: u8,
}

impl Default for ChessState {
    fn default() -> Self {
        let mut board = [0_u8; BOARD_LEN];
        board[..8].copy_from_slice(&[4, 2, 3, 5, 6, 3, 2, 4]);
        board[8..16].fill(1);
        board[48..56].fill(7);
        board[56..].copy_from_slice(&[10, 8, 9, 11, 12, 9, 8, 10]);
        Self {
            board,
            turn: Color::White,
            ply: 0,
        }
    }
}

impl ChessState {
    pub fn apply_move(&self, notation: &str) -> Result<(Self, AppliedMove), String> {
        let notation = notation.trim().to_ascii_lowercase();
        if notation.len() != 4 {
            return Err("use coordinate notation such as e2e4".to_owned());
        }
        let from = parse_square(&notation[..2])?;
        let to = parse_square(&notation[2..])?;
        if from == to {
            return Err("source and destination must differ".to_owned());
        }
        let piece = self.board[from];
        if piece == 0 {
            return Err(format!("{} is empty", &notation[..2]));
        }
        if piece_color(piece) != Some(self.turn) {
            return Err(format!("it is {} to move", self.turn.label()));
        }
        if piece_color(self.board[to]) == Some(self.turn) {
            return Err("destination contains a friendly piece".to_owned());
        }
        if !self.valid_geometry(piece, from, to) {
            return Err(format!("{notation} is not a valid movement for that piece"));
        }

        let mut next = self.clone();
        next.board[from] = 0;
        next.board[to] = promoted_piece(piece, to);
        next.turn = self.turn.other();
        next.ply = self.ply.saturating_add(1);
        let committed_piece = next.board[to];
        Ok((
            next,
            AppliedMove {
                from,
                to,
                piece: committed_piece,
            },
        ))
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(81);
        bytes.extend_from_slice(STATE_MAGIC);
        bytes.push(u8::from(self.turn == Color::Black));
        bytes.extend_from_slice(&self.ply.to_be_bytes());
        bytes.extend_from_slice(&self.board);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != 81 || &bytes[..8] != STATE_MAGIC {
            return Err("invalid Chess state record".to_owned());
        }
        let turn = match bytes[8] {
            0 => Color::White,
            1 => Color::Black,
            _ => return Err("invalid side-to-move value".to_owned()),
        };
        let mut ply = [0_u8; 8];
        ply.copy_from_slice(&bytes[9..17]);
        let mut board = [0_u8; BOARD_LEN];
        board.copy_from_slice(&bytes[17..]);
        Ok(Self {
            board,
            turn,
            ply: u64::from_be_bytes(ply),
        })
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

    fn valid_geometry(&self, piece: u8, from: usize, to: usize) -> bool {
        let (from_file, from_rank) = coordinates(from);
        let (to_file, to_rank) = coordinates(to);
        let dx = to_file - from_file;
        let dy = to_rank - from_rank;
        match normalized_piece(piece) {
            1 => self.valid_pawn(piece, from_file, from_rank, to_file, to_rank, to),
            2 => (dx.abs() == 1 && dy.abs() == 2) || (dx.abs() == 2 && dy.abs() == 1),
            3 => dx.abs() == dy.abs() && self.path_is_clear(from_file, from_rank, to_file, to_rank),
            4 => (dx == 0 || dy == 0) && self.path_is_clear(from_file, from_rank, to_file, to_rank),
            5 => {
                (dx == 0 || dy == 0 || dx.abs() == dy.abs())
                    && self.path_is_clear(from_file, from_rank, to_file, to_rank)
            }
            6 => dx.abs() <= 1 && dy.abs() <= 1,
            _ => false,
        }
    }

    fn valid_pawn(
        &self,
        piece: u8,
        from_file: i32,
        from_rank: i32,
        to_file: i32,
        to_rank: i32,
        to: usize,
    ) -> bool {
        let (direction, start_rank) = if piece_color(piece) == Some(Color::White) {
            (1, 1)
        } else {
            (-1, 6)
        };
        let dx = to_file - from_file;
        let dy = to_rank - from_rank;
        if dx == 0 && self.board[to] == 0 {
            if dy == direction {
                return true;
            }
            if from_rank == start_rank && dy == direction * 2 {
                let middle = square_index(from_file, from_rank + direction);
                return self.board[middle] == 0;
            }
        }
        dx.abs() == 1 && dy == direction && self.board[to] != 0
    }

    fn path_is_clear(&self, from_file: i32, from_rank: i32, to_file: i32, to_rank: i32) -> bool {
        let step_file = (to_file - from_file).signum();
        let step_rank = (to_rank - from_rank).signum();
        let mut file = from_file + step_file;
        let mut rank = from_rank + step_rank;
        while file != to_file || rank != to_rank {
            if self.board[square_index(file, rank)] != 0 {
                return false;
            }
            file += step_file;
            rank += step_rank;
        }
        true
    }
}

#[must_use]
pub const fn piece_code(piece: u8) -> char {
    match piece {
        1 => 'P',
        2 => 'N',
        3 => 'B',
        4 => 'R',
        5 => 'Q',
        6 => 'K',
        7 => 'p',
        8 => 'n',
        9 => 'b',
        10 => 'r',
        11 => 'q',
        12 => 'k',
        _ => '.',
    }
}

#[must_use]
pub fn square_name(index: usize) -> String {
    let file = char::from(b'a' + u8::try_from(index % 8).expect("file fits u8"));
    let rank = char::from(b'1' + u8::try_from(index / 8).expect("rank fits u8"));
    format!("{file}{rank}")
}

fn parse_square(value: &str) -> Result<usize, String> {
    let bytes = value.as_bytes();
    if bytes.len() != 2 || !(b'a'..=b'h').contains(&bytes[0]) || !(b'1'..=b'8').contains(&bytes[1])
    {
        return Err(format!("invalid square {value:?}"));
    }
    Ok(usize::from(bytes[1] - b'1') * 8 + usize::from(bytes[0] - b'a'))
}

const fn piece_color(piece: u8) -> Option<Color> {
    match piece {
        1..=6 => Some(Color::White),
        7..=12 => Some(Color::Black),
        _ => None,
    }
}

const fn normalized_piece(piece: u8) -> u8 {
    if piece > 6 {
        piece - 6
    } else {
        piece
    }
}

const fn promoted_piece(piece: u8, to: usize) -> u8 {
    match (piece, to / 8) {
        (1, 7) => 5,
        (7, 0) => 11,
        _ => piece,
    }
}

fn coordinates(index: usize) -> (i32, i32) {
    (
        i32::try_from(index % 8).expect("file fits i32"),
        i32::try_from(index / 8).expect("rank fits i32"),
    )
}

fn square_index(file: i32, rank: i32) -> usize {
    usize::try_from(rank * 8 + file).expect("validated board coordinate")
}

#[cfg(test)]
mod tests {
    use super::ChessState;

    #[test]
    fn legal_opening_round_trips() {
        let state = ChessState::default();
        let (state, _) = state.apply_move("e2e4").expect("white pawn moves");
        let (state, _) = state.apply_move("e7e5").expect("black pawn moves");
        assert_eq!(
            state,
            ChessState::decode(&state.encode()).expect("state decodes")
        );
    }

    #[test]
    fn rejects_blocked_rook() {
        assert!(ChessState::default().apply_move("a1a4").is_err());
    }
}
