use rust_decimal::Decimal;

pub struct BookLevelUpdate {
    pub price: Decimal,
    pub quantity: Decimal,
}

pub struct BookUpdate {
    pub bids: Vec<BookLevelUpdate>,
    pub asks: Vec<BookLevelUpdate>,
}