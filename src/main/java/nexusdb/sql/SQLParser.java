package nexusdb.sql;

import java.util.*;

/**
 * Hand-written recursive descent SQL parser.
 *
 * Parses a subset of SQL into an AST represented by the {@link SQLStatement} sealed interface.
 * Entry point: {@link #parse(String)}.
 */
public final class SQLParser {

    // =========================================================
    // Public API
    // =========================================================

    /** Parse a SQL string and return the corresponding {@link SQLStatement}. */
    public SQLStatement parse(String sql) {
        List<Token> tokens = tokenize(sql);
        return new Parser(tokens).parseStatement();
    }

    // =========================================================
    // AST types
    // =========================================================

    /** Top-level sealed interface for all SQL statement types. */
    public sealed interface SQLStatement
            permits SQLStatement.SelectStatement,
                    SQLStatement.InsertStatement,
                    SQLStatement.UpdateStatement,
                    SQLStatement.DeleteStatement,
                    SQLStatement.CreateTableStatement,
                    SQLStatement.CreateIndexStatement,
                    SQLStatement.ExplainStatement {

        record SelectStatement(
                String table,
                List<String> columns,       // empty = SELECT *
                WhereClause where,          // null = no WHERE
                OrderByClause orderBy       // null = no ORDER BY
        ) implements SQLStatement {}

        record InsertStatement(
                String table,
                List<String> columns,
                List<List<String>> rows     // one or more value rows
        ) implements SQLStatement {}

        record UpdateStatement(
                String table,
                Map<String, String> setClause,
                WhereClause where
        ) implements SQLStatement {}

        record DeleteStatement(
                String table,
                WhereClause where
        ) implements SQLStatement {}

        record CreateTableStatement(
                String table,
                List<ColumnDef> columns
        ) implements SQLStatement {}

        record CreateIndexStatement(
                String indexName,
                String table,
                List<String> columns
        ) implements SQLStatement {}

        record ExplainStatement(SQLStatement inner) implements SQLStatement {}
    }

    /** Column definition used in CREATE TABLE. */
    public record ColumnDef(String name, ColumnType type, boolean isPrimaryKey, boolean isNotNull) {}

    /** Supported column types. */
    public enum ColumnType { INT, VARCHAR, BOOLEAN }

    /** ORDER BY clause. */
    public record OrderByClause(String column, boolean ascending) {}

    // =========================================================
    // WhereClause hierarchy
    // =========================================================

    /** Sealed interface for WHERE predicates. */
    public sealed interface WhereClause
            permits WhereClause.And,
                    WhereClause.Or,
                    WhereClause.Comparison {

        record And(WhereClause left, WhereClause right) implements WhereClause {}
        record Or(WhereClause left, WhereClause right)  implements WhereClause {}
        record Comparison(String column, Operator operator, String value) implements WhereClause {}
    }

    /** Comparison operators. */
    public enum Operator { EQ, NE, LT, GT, LE, GE }

    // =========================================================
    // Tokenizer
    // =========================================================

    enum TokenType {
        KEYWORD, IDENTIFIER, INTEGER_LITERAL, STRING_LITERAL, BOOLEAN_LITERAL,
        OPERATOR, LPAREN, RPAREN, COMMA, SEMICOLON, DOT, STAR, EOF
    }

    record Token(TokenType type, String value) {
        boolean is(String v)       { return value.equalsIgnoreCase(v); }
        boolean isType(TokenType t){ return type == t; }
    }

    /** Tokenize a SQL string into a list of tokens. */
    static List<Token> tokenize(String sql) {
        List<Token> tokens = new ArrayList<>();
        int i = 0;
        int len = sql.length();

        while (i < len) {
            char c = sql.charAt(i);

            // Whitespace
            if (Character.isWhitespace(c)) { i++; continue; }

            // String literal
            if (c == '\'') {
                int start = i + 1;
                i++;
                while (i < len && sql.charAt(i) != '\'') {
                    if (sql.charAt(i) == '\\') i++; // skip escape
                    i++;
                }
                if (i >= len) throw new SQLParseException("Unterminated string literal starting at position " + start);
                tokens.add(new Token(TokenType.STRING_LITERAL, sql.substring(start, i)));
                i++; // consume closing quote
                continue;
            }

            // Single-char tokens
            if (c == '(') { tokens.add(new Token(TokenType.LPAREN,  "(")); i++; continue; }
            if (c == ')') { tokens.add(new Token(TokenType.RPAREN,  ")")); i++; continue; }
            if (c == ',') { tokens.add(new Token(TokenType.COMMA,   ",")); i++; continue; }
            if (c == ';') { tokens.add(new Token(TokenType.SEMICOLON,";")); i++; continue; }
            if (c == '.') { tokens.add(new Token(TokenType.DOT,     ".")); i++; continue; }
            if (c == '*') { tokens.add(new Token(TokenType.STAR,    "*")); i++; continue; }

            // Operators
            if (c == '=' ) { tokens.add(new Token(TokenType.OPERATOR, "=")); i++; continue; }
            if (c == '<') {
                if (i + 1 < len && sql.charAt(i+1) == '=') {
                    tokens.add(new Token(TokenType.OPERATOR, "<=")); i += 2;
                } else if (i + 1 < len && sql.charAt(i+1) == '>') {
                    tokens.add(new Token(TokenType.OPERATOR, "<>")); i += 2;
                } else {
                    tokens.add(new Token(TokenType.OPERATOR, "<")); i++;
                }
                continue;
            }
            if (c == '>') {
                if (i + 1 < len && sql.charAt(i+1) == '=') {
                    tokens.add(new Token(TokenType.OPERATOR, ">=")); i += 2;
                } else {
                    tokens.add(new Token(TokenType.OPERATOR, ">")); i++;
                }
                continue;
            }
            if (c == '!') {
                if (i + 1 < len && sql.charAt(i+1) == '=') {
                    tokens.add(new Token(TokenType.OPERATOR, "!=")); i += 2;
                } else {
                    throw new SQLParseException("Unexpected character '!' at position " + i);
                }
                continue;
            }

            // Numbers
            if (Character.isDigit(c) || (c == '-' && i + 1 < len && Character.isDigit(sql.charAt(i+1)))) {
                int start = i;
                if (c == '-') i++;
                while (i < len && Character.isDigit(sql.charAt(i))) i++;
                tokens.add(new Token(TokenType.INTEGER_LITERAL, sql.substring(start, i)));
                continue;
            }

            // Identifiers / keywords
            if (Character.isLetter(c) || c == '_') {
                int start = i;
                while (i < len && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_')) i++;
                String word = sql.substring(start, i);
                tokens.add(classifyWord(word));
                continue;
            }

            throw new SQLParseException("Unexpected character '" + c + "' at position " + i);
        }

        tokens.add(new Token(TokenType.EOF, ""));
        return tokens;
    }

    private static final Set<String> KEYWORDS = Set.of(
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
        "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "INDEX",
        "ON", "PRIMARY", "KEY", "NOT", "NULL", "AND", "OR",
        "ORDER", "BY", "ASC", "DESC", "INT", "VARCHAR", "BOOLEAN",
        "EXPLAIN", "ALL", "DISTINCT"
    );

    private static Token classifyWord(String word) {
        String upper = word.toUpperCase(Locale.ROOT);
        if ("true".equalsIgnoreCase(word) || "false".equalsIgnoreCase(word)) {
            return new Token(TokenType.BOOLEAN_LITERAL, word.toLowerCase(Locale.ROOT));
        }
        if (KEYWORDS.contains(upper)) {
            return new Token(TokenType.KEYWORD, upper);
        }
        return new Token(TokenType.IDENTIFIER, word);
    }

    // =========================================================
    // Parser
    // =========================================================

    private static final class Parser {
        private final List<Token> tokens;
        private int pos = 0;

        Parser(List<Token> tokens) { this.tokens = tokens; }

        SQLStatement parseStatement() {
            Token t = peek();
            SQLStatement stmt = switch (t.value().toUpperCase(Locale.ROOT)) {
                case "SELECT" -> parseSelect();
                case "INSERT" -> parseInsert();
                case "UPDATE" -> parseUpdate();
                case "DELETE" -> parseDelete();
                case "CREATE" -> parseCreate();
                case "EXPLAIN" -> parseExplain();
                default -> throw new SQLParseException("Unknown statement type: " + t.value());
            };
            // optional trailing semicolon
            if (peek().isType(TokenType.SEMICOLON)) advance();
            return stmt;
        }

        // SELECT [DISTINCT] (* | col, ...) FROM table [WHERE ...] [ORDER BY col [ASC|DESC]]
        private SQLStatement.SelectStatement parseSelect() {
            consume("SELECT");
            // optional DISTINCT (ignored for now but consumed)
            if (peek().is("DISTINCT")) advance();

            List<String> columns = parseColumnList();

            if (!peek().is("FROM")) {
                throw new SQLParseException("Expected FROM after column list");
            }
            consume("FROM");
            String table = parseIdentifier();
            WhereClause where = parseOptionalWhere();
            OrderByClause orderBy = parseOptionalOrderBy();

            return new SQLStatement.SelectStatement(table, columns, where, orderBy);
        }

        // INSERT INTO table (cols) VALUES (vals) [, (vals)]*
        private SQLStatement.InsertStatement parseInsert() {
            consume("INSERT");
            consume("INTO");
            String table = parseIdentifier();
            consume("(");
            List<String> cols = parseIdentifierList();
            consume(")");
            consume("VALUES");
            List<List<String>> rows = new ArrayList<>();
            rows.add(parseValueRow());
            while (peek().isType(TokenType.COMMA)) {
                advance();
                rows.add(parseValueRow());
            }
            return new SQLStatement.InsertStatement(table, cols, rows);
        }

        // UPDATE table SET col=val [, col=val]* [WHERE ...]
        private SQLStatement.UpdateStatement parseUpdate() {
            consume("UPDATE");
            String table = parseIdentifier();
            consume("SET");
            Map<String, String> setClause = new LinkedHashMap<>();
            do {
                String col = parseIdentifier();
                consume("=");
                String val = parseValue();
                setClause.put(col, val);
            } while (peek().isType(TokenType.COMMA) && advance() != null);
            WhereClause where = parseOptionalWhere();
            return new SQLStatement.UpdateStatement(table, setClause, where);
        }

        // DELETE FROM table [WHERE ...]
        private SQLStatement.DeleteStatement parseDelete() {
            consume("DELETE");
            consume("FROM");
            String table = parseIdentifier();
            WhereClause where = parseOptionalWhere();
            return new SQLStatement.DeleteStatement(table, where);
        }

        // CREATE TABLE table (col type [PRIMARY KEY] [NOT NULL], ...)
        // CREATE INDEX name ON table (col, ...)
        private SQLStatement parseCreate() {
            consume("CREATE");
            if (peek().is("TABLE")) {
                consume("TABLE");
                String table = parseIdentifier();
                consume("(");
                List<ColumnDef> cols = new ArrayList<>();
                cols.add(parseColumnDef());
                while (peek().isType(TokenType.COMMA)) {
                    advance();
                    cols.add(parseColumnDef());
                }
                consume(")");
                return new SQLStatement.CreateTableStatement(table, cols);
            } else if (peek().is("INDEX")) {
                consume("INDEX");
                String indexName = parseIdentifier();
                consume("ON");
                String table = parseIdentifier();
                consume("(");
                List<String> cols = parseIdentifierList();
                consume(")");
                return new SQLStatement.CreateIndexStatement(indexName, table, cols);
            } else {
                throw new SQLParseException("Expected TABLE or INDEX after CREATE, got: " + peek().value());
            }
        }

        // EXPLAIN <statement>
        private SQLStatement.ExplainStatement parseExplain() {
            consume("EXPLAIN");
            SQLStatement inner = parseStatement();
            return new SQLStatement.ExplainStatement(inner);
        }

        // ---- Column list: * or col [, col]*
        private List<String> parseColumnList() {
            if (peek().isType(TokenType.STAR)) {
                advance();
                return Collections.emptyList(); // empty = SELECT *
            }
            return parseQualifiedIdentifierList();
        }

        // col or table.col
        private List<String> parseQualifiedIdentifierList() {
            List<String> cols = new ArrayList<>();
            cols.add(parseQualifiedIdentifier());
            while (peek().isType(TokenType.COMMA)) {
                advance();
                cols.add(parseQualifiedIdentifier());
            }
            return cols;
        }

        private String parseQualifiedIdentifier() {
            String name = parseIdentifier();
            if (peek().isType(TokenType.DOT)) {
                advance();
                String col = parseIdentifier();
                return name + "." + col;
            }
            return name;
        }

        // plain identifier list (no table prefix)
        private List<String> parseIdentifierList() {
            List<String> ids = new ArrayList<>();
            ids.add(parseIdentifier());
            while (peek().isType(TokenType.COMMA)) {
                advance();
                ids.add(parseIdentifier());
            }
            return ids;
        }

        // (val, val, ...)
        private List<String> parseValueRow() {
            consume("(");
            List<String> vals = new ArrayList<>();
            vals.add(parseValue());
            while (peek().isType(TokenType.COMMA)) {
                advance();
                vals.add(parseValue());
            }
            consume(")");
            return vals;
        }

        // col type [PRIMARY KEY] [NOT NULL]
        private ColumnDef parseColumnDef() {
            String name = parseIdentifier();
            ColumnType type = parseColumnType();
            boolean isPrimaryKey = false;
            boolean isNotNull = false;
            while (true) {
                if (peek().is("PRIMARY")) {
                    advance(); consume("KEY"); isPrimaryKey = true; isNotNull = true;
                } else if (peek().is("NOT")) {
                    advance(); consume("NULL"); isNotNull = true;
                } else {
                    break;
                }
            }
            return new ColumnDef(name, type, isPrimaryKey, isNotNull);
        }

        private ColumnType parseColumnType() {
            Token t = peek();
            if (t.is("INT")) { advance(); return ColumnType.INT; }
            if (t.is("VARCHAR")) { advance(); return ColumnType.VARCHAR; }
            if (t.is("BOOLEAN")) { advance(); return ColumnType.BOOLEAN; }
            throw new SQLParseException("Expected column type (INT, VARCHAR, BOOLEAN), got: " + t.value());
        }

        // ---- WHERE clause parsing

        private WhereClause parseOptionalWhere() {
            if (!peek().is("WHERE")) return null;
            advance();
            return parseOrExpr();
        }

        // OR has lower precedence than AND
        private WhereClause parseOrExpr() {
            WhereClause left = parseAndExpr();
            while (peek().is("OR")) {
                advance();
                WhereClause right = parseAndExpr();
                left = new WhereClause.Or(left, right);
            }
            return left;
        }

        private WhereClause parseAndExpr() {
            WhereClause left = parsePredicate();
            while (peek().is("AND")) {
                advance();
                WhereClause right = parsePredicate();
                left = new WhereClause.And(left, right);
            }
            return left;
        }

        private WhereClause parsePredicate() {
            if (peek().isType(TokenType.LPAREN)) {
                advance();
                WhereClause inner = parseOrExpr();
                consume(")");
                return inner;
            }
            String col = parseQualifiedIdentifier();
            Operator op = parseOperator();
            String val = parseValue();
            return new WhereClause.Comparison(col, op, val);
        }

        private Operator parseOperator() {
            Token t = peek();
            if (!t.isType(TokenType.OPERATOR)) {
                throw new SQLParseException("Expected comparison operator, got: " + t.value());
            }
            advance();
            return switch (t.value()) {
                case "="  -> Operator.EQ;
                case "!=" -> Operator.NE;
                case "<>" -> Operator.NE;
                case "<"  -> Operator.LT;
                case ">"  -> Operator.GT;
                case "<=" -> Operator.LE;
                case ">=" -> Operator.GE;
                default   -> throw new SQLParseException("Invalid operator: " + t.value());
            };
        }

        private OrderByClause parseOptionalOrderBy() {
            if (!peek().is("ORDER")) return null;
            advance();
            consume("BY");
            String col = parseQualifiedIdentifier();
            boolean ascending = true;
            if (peek().is("DESC")) { advance(); ascending = false; }
            else if (peek().is("ASC")) { advance(); }
            return new OrderByClause(col, ascending);
        }

        // ---- Value parsing: string, integer, boolean, or identifier

        private String parseValue() {
            Token t = peek();
            return switch (t.type()) {
                case STRING_LITERAL  -> { advance(); yield t.value(); }
                case INTEGER_LITERAL -> { advance(); yield t.value(); }
                case BOOLEAN_LITERAL -> { advance(); yield t.value(); }
                case IDENTIFIER      -> { advance(); yield t.value(); }
                case KEYWORD         -> {
                    // allow NULL as a value
                    if (t.is("NULL")) { advance(); yield "null"; }
                    throw new SQLParseException("Unexpected keyword as value: " + t.value());
                }
                default -> throw new SQLParseException("Expected value, got: " + t.value() + " (" + t.type() + ")");
            };
        }

        private String parseIdentifier() {
            Token t = peek();
            if (t.isType(TokenType.IDENTIFIER) || t.isType(TokenType.KEYWORD)) {
                // Allow keywords used as identifiers (e.g., table named "index")
                advance();
                return t.value();
            }
            throw new SQLParseException("Expected identifier, got: " + t.value() + " (" + t.type() + ")");
        }

        // ---- Token navigation helpers

        private Token peek() { return tokens.get(pos); }

        private Token advance() {
            Token t = tokens.get(pos);
            if (t.type() != TokenType.EOF) pos++;
            return t;
        }

        /** Consume a token matching the given value (case-insensitive). */
        private void consume(String value) {
            Token t = peek();
            if (t.isType(TokenType.EOF)) {
                throw new SQLParseException("Expected '" + value + "' but reached end of input");
            }
            if (value.equals("(") && t.isType(TokenType.LPAREN)) { advance(); return; }
            if (value.equals(")") && t.isType(TokenType.RPAREN)) { advance(); return; }
            if (value.equals(",") && t.isType(TokenType.COMMA))  { advance(); return; }
            if (value.equals("=") && t.isType(TokenType.OPERATOR) && t.value().equals("=")) { advance(); return; }
            if (t.value().equalsIgnoreCase(value)) { advance(); return; }
            throw new SQLParseException("Expected '" + value + "' but got '" + t.value() + "'");
        }
    }
}
