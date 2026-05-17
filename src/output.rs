use datafusion::arrow::array::{Array, RecordBatch};

fn array_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }

    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::*;

    match array.data_type() {
        DataType::Int8 => array.as_primitive::<Int8Type>().value(row).to_string(),
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row).to_string(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::UInt8 => array.as_primitive::<UInt8Type>().value(row).to_string(),
        DataType::UInt16 => array.as_primitive::<UInt16Type>().value(row).to_string(),
        DataType::UInt32 => array.as_primitive::<UInt32Type>().value(row).to_string(),
        DataType::UInt64 => array.as_primitive::<UInt64Type>().value(row).to_string(),
        DataType::Float32 => format!("{:.4}", array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => format!("{:.4}", array.as_primitive::<Float64Type>().value(row)),
        DataType::Boolean => array.as_boolean().value(row).to_string(),
        DataType::Utf8 => array.as_string::<i32>().value(row).to_string(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).to_string(),
        DataType::Utf8View => array.as_string_view().value(row).to_string(),
        DataType::Binary => format!("{:?}", array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => format!("{:?}", array.as_binary::<i64>().value(row)),
        DataType::BinaryView => format!("{:?}", array.as_binary_view().value(row)),
        _ => format!("{array:?}"),
    }
}

pub struct TablePrinter {
    col_widths: Vec<usize>,
}

impl TablePrinter {
    pub fn new() -> Self {
        Self {
            col_widths: Vec::new(),
        }
    }

    pub fn print_batches(&mut self, batches: &[RecordBatch]) {
        if batches.is_empty() {
            println!("(empty result)");
            return;
        }

        let schema = batches[0].schema();
        let fields = schema.fields();
        let num_cols = fields.len();
        let headers: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();

        self.col_widths = fields.iter().map(|f| f.name().len()).collect();

        for batch in batches {
            for col_idx in 0..num_cols {
                let col = batch.column(col_idx);
                for row_idx in 0..batch.num_rows() {
                    let cell = self.format_value(col.as_ref(), row_idx);
                    if let Some(w) = self.col_widths.get_mut(col_idx) {
                        *w = (*w).max(cell.len());
                    }
                }
            }
        }

        self.print_line();
        self.print_row(headers);
        self.print_line();

        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let cells: Vec<String> = (0..batch.num_columns())
                    .map(|col_idx| self.format_value(batch.column(col_idx).as_ref(), row_idx))
                    .collect();
                self.print_row(cells);
            }
        }

        self.print_line();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!(
            "({} row{})",
            total_rows,
            if total_rows == 1 { "" } else { "s" }
        );
        println!();
    }

    pub fn print_table(&mut self, headers: &[&str], rows: &[Vec<String>]) {
        self.col_widths = headers.iter().map(|h| h.len()).collect();

        for row in rows {
            for (i, cell) in row.iter().enumerate() {
                if let Some(w) = self.col_widths.get_mut(i) {
                    *w = (*w).max(cell.len());
                }
            }
        }

        self.print_line();
        self.print_row(headers.iter().map(|s| s.to_string()).collect());
        self.print_line();

        for row in rows {
            self.print_row(row.clone());
        }

        self.print_line();
        println!();
    }

    fn format_value(&self, array: &dyn Array, row: usize) -> String {
        if row >= array.len() {
            return String::new();
        }
        array_to_string(array, row)
    }

    fn print_line(&self) {
        print!("+");
        for &w in &self.col_widths {
            print!("{}+", "-".repeat(w + 2));
        }
        println!("+");
    }

    fn print_row(&self, cells: Vec<String>) {
        print!("|");
        for (i, cell) in cells.iter().enumerate() {
            let w = self.col_widths.get(i).copied().unwrap_or(cell.len());
            print!(" {:w$} |", cell, w = w);
        }
        println!();
    }
}

impl Default for TablePrinter {
    fn default() -> Self {
        Self::new()
    }
}
