package com.annihilator.data.playground.utility;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

public class SQLQueryFieldExtractorTest {

  @Test
  public void testComplexQueryWithCoalesceSum() {
    String complexQuery =
        "SELECT user_id, product_id, order_id, "
            + "coalesce(sum(quantity), 0) as total_qty, "
            + "coalesce(sum(price), 0), "
            + "coalesce(sum(discount), 0), "
            + "coalesce(sum(tax), 0), "
            + "product_info.category as product_category "
            + "FROM sales_data "
            + "WHERE order_date = '2025-08-25' "
            + "GROUP BY user_id, product_id, order_id "
            + "LIMIT 100";

    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(complexQuery);

    // Verify that the extraction doesn't fail and returns some fields
    assertNotNull(fields, "Extracted fields should not be null");
    assertFalse(fields.isEmpty(), "Should extract some fields from the complex query");

    // Print the extracted fields for debugging
    // Extracted fields count: expected 8
    // First 10 fields: [user_id, product_id, order_id, total_qty, coalesce(sum(price), 0),
    // coalesce(sum(discount), 0), coalesce(sum(tax), 0), product_category]

    // Verify some expected fields are present
    assertTrue(fields.contains("user_id"), "Should contain user_id");
    assertTrue(fields.contains("product_id"), "Should contain product_id");
    assertTrue(fields.contains("order_id"), "Should contain order_id");

    // Verify coalesce functions are handled properly
    assertTrue(
        fields.stream().anyMatch(field -> field.contains("coalesce")),
        "Should contain coalesce function results");
  }

  @Test
  public void testSimpleQuery() {
    String simpleQuery = "SELECT id, name, email FROM users WHERE active = 1";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(simpleQuery);

    assertEquals(3, fields.size());
    assertTrue(fields.contains("id"));
    assertTrue(fields.contains("name"));
    assertTrue(fields.contains("email"));
  }

  @Test
  public void testQueryWithAliases() {
    String queryWithAliases = "SELECT u.id as user_id, u.name as user_name FROM users u";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(queryWithAliases);

    assertEquals(2, fields.size());
    assertTrue(fields.contains("user_id"));
    assertTrue(fields.contains("user_name"));
  }

  @Test
  public void testSelectStar() {
    String selectStarQuery = "SELECT * FROM users WHERE active = 1";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(selectStarQuery);

    assertTrue(fields.isEmpty(), "SELECT * should return empty list");
  }

  @Test
  public void testSelectStarWithDistinct() {
    String selectStarQuery = "SELECT DISTINCT * FROM users";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(selectStarQuery);

    assertTrue(fields.isEmpty(), "SELECT DISTINCT * should return empty list");
  }

  @Test
  public void testNoConflicts_PrefixRemoval() {
    // Scenario: Single table with prefixed columns - should remove prefixes
    String query = "SELECT a.account_id, a.billing_timezone, a.user_name FROM accounts a";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(3, fields.size());
    assertTrue(fields.contains("account_id"), "Should remove prefix from account_id");
    assertTrue(fields.contains("billing_timezone"), "Should remove prefix from billing_timezone");
    assertTrue(fields.contains("user_name"), "Should remove prefix from user_name");
  }

  @Test
  public void testConflicts_PrefixPreservation() {
    // Scenario: Multiple tables with same column names - should keep prefixes
    String query =
        "SELECT a.account_id, b.account_id, a.user_id, c.user_id FROM accounts a, users b, profiles c";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(4, fields.size());
    assertTrue(fields.contains("a.account_id"), "Should keep prefix for conflicting account_id");
    assertTrue(fields.contains("b.account_id"), "Should keep prefix for conflicting account_id");
    assertTrue(fields.contains("a.user_id"), "Should keep prefix for conflicting user_id");
    assertTrue(fields.contains("c.user_id"), "Should keep prefix for conflicting user_id");
  }

  @Test
  public void testMixedScenario_SafeAndConflicting() {
    // Scenario: Mix of safe and conflicting fields
    String query =
        "SELECT a.account_id, a.billing_timezone, b.account_id, c.user_name FROM accounts a, users b, profiles c";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(4, fields.size());
    assertTrue(fields.contains("a.account_id"), "Should keep prefix for conflicting account_id");
    assertTrue(
        fields.contains("billing_timezone"), "Should remove prefix from safe billing_timezone");
    assertTrue(fields.contains("b.account_id"), "Should keep prefix for conflicting account_id");
    assertTrue(fields.contains("user_name"), "Should remove prefix from safe user_name");
  }

  @Test
  public void testFunctionsWithPrefixes() {
    // Scenario: Functions with table prefixes
    String query =
        "SELECT SUM(a.amount) AS total_amount, AVG(b.price) AS avg_price FROM orders a, products b";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(2, fields.size());
    assertTrue(fields.contains("total_amount"), "Should use explicit alias");
    assertTrue(fields.contains("avg_price"), "Should use explicit alias");
  }

  @Test
  public void testFunctionsWithoutAliases() {
    // Scenario: Functions without explicit aliases
    String query = "SELECT SUM(a.amount), AVG(b.price) FROM orders a, products b";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(2, fields.size());
    assertTrue(fields.contains("SUM(a.amount)"), "Should preserve full function expression");
    assertTrue(fields.contains("AVG(b.price)"), "Should preserve full function expression");
  }

  @Test
  public void testComplexQueryWithMixedPrefixes() {
    // Scenario: Complex query like the user's example
    String query =
        "SELECT a.account_id, a.billing_timezone, "
            + "SUM(CASE WHEN prc.prc_rg_id IN ('VIDEO', 'DISPL') THEN raf.tubemogul_media_cost_passthrough_micros_acur ELSE 0 END) / 1000000.0 AS media_cost_passthrough_acur, "
            + "SUM(billable_fee_micros_acur) / 1000000.0 AS billable_fee_acur "
            + "FROM rollup_ad_fee_orc raf "
            + "INNER JOIN account a ON raf.account_id = a.account_id "
            + "INNER JOIN prc_rg_mapping prc ON raf.service_type = prc.service_type "
            + "GROUP BY a.account_id, a.billing_timezone";

    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(4, fields.size());
    assertTrue(fields.contains("account_id"), "Should remove prefix from account_id (no conflict)");
    assertTrue(
        fields.contains("billing_timezone"),
        "Should remove prefix from billing_timezone (no conflict)");
    assertTrue(fields.contains("media_cost_passthrough_acur"), "Should use explicit alias");
    assertTrue(fields.contains("billable_fee_acur"), "Should use explicit alias");
  }

  @Test
  public void testSameTableMultipleReferences() {
    // Scenario: Same table referenced multiple times - should be safe to remove prefix
    String query = "SELECT a.account_id, a.account_id FROM accounts a";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(2, fields.size());
    assertTrue(
        fields.contains("account_id"), "Should remove prefix even with duplicate references");
    assertTrue(fields.contains("account_id"), "Should have duplicate account_id entries");
  }

  @Test
  public void testNestedFunctionsWithPrefixes() {
    // Scenario: Nested functions with table prefixes
    String query =
        "SELECT COALESCE(SUM(a.amount), 0) AS total, MAX(b.price) AS max_price FROM orders a, products b";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(2, fields.size());
    assertTrue(fields.contains("total"), "Should use explicit alias");
    assertTrue(fields.contains("max_price"), "Should use explicit alias");
  }

  @Test
  public void testCaseStatementsWithPrefixes() {
    // Scenario: CASE statements with table prefixes
    String query =
        "SELECT CASE WHEN a.status = 'active' THEN a.amount ELSE 0 END AS active_amount FROM accounts a";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(1, fields.size());
    assertTrue(fields.contains("active_amount"), "Should use explicit alias");
  }

  @Test
  public void testWindowFunctionsWithPrefixes() {
    // Scenario: Window functions with table prefixes
    String query =
        "SELECT a.account_id, ROW_NUMBER() OVER (PARTITION BY a.status ORDER BY a.amount) AS row_num FROM accounts a";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(2, fields.size());
    assertTrue(fields.contains("account_id"), "Should remove prefix from account_id");
    assertTrue(fields.contains("row_num"), "Should use explicit alias");
  }

  @Test
  public void testSubqueriesWithPrefixes() {
    // Scenario: Subqueries with table prefixes
    String query =
        "SELECT a.account_id, (SELECT COUNT(*) FROM orders o WHERE o.account_id = a.account_id) AS order_count FROM accounts a";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(2, fields.size());
    assertTrue(fields.contains("account_id"), "Should remove prefix from account_id");
    assertTrue(fields.contains("order_count"), "Should use explicit alias");
  }

  @Test
  public void testEdgeCase_EmptyFields() {
    // Scenario: Empty or whitespace fields
    String query = "SELECT a.account_id, , b.user_id FROM accounts a, users b";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(2, fields.size());
    assertTrue(fields.contains("account_id"), "Should handle empty fields gracefully");
    assertTrue(fields.contains("user_id"), "Should handle empty fields gracefully");
  }

  @Test
  public void testEdgeCase_SpecialCharacters() {
    // Scenario: Fields with special characters
    String query = "SELECT a.account_id, a.\"user-name\", a.user_name FROM accounts a";
    List<String> fields = SQLQueryFieldExtractor.extractSelectedFields(query);

    assertEquals(3, fields.size());
    assertTrue(fields.contains("account_id"), "Should handle special characters");
    assertTrue(fields.contains("\"user-name\""), "Should preserve quoted field names");
    assertTrue(fields.contains("user_name"), "Should handle regular field names");
  }
}
