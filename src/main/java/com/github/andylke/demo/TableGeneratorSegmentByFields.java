package com.github.andylke.demo;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.engine.spi.SessionEventListenerManager;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.Configurable;
import org.hibernate.id.ExportableColumn;
import org.hibernate.id.IdentifierGenerationException;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.id.enhanced.AccessCallback;
import org.hibernate.id.enhanced.Optimizer;
import org.hibernate.id.enhanced.OptimizerFactory;
import org.hibernate.id.enhanced.StandardOptimizerDescriptor;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.jdbc.AbstractReturningWork;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.Table;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.LongType;
import org.hibernate.type.StringType;
import org.hibernate.type.Type;
import org.jboss.logging.Logger;

public class TableGeneratorSegmentByFields implements PersistentIdentifierGenerator, Configurable {

  private static final CoreMessageLogger LOG =
      Logger.getMessageLogger(
          CoreMessageLogger.class, TableGeneratorSegmentByFields.class.getName());

  public static final String PARAM_SEPARATOR_STRING = ",";

  /** Configures the name of the table to use. The default value is {@link #DEFAULT_TABLE} */
  public static final String TABLE_PARAM = "table_name";

  /** The default {@link #TABLE_PARAM} value */
  public static final String DEFAULT_TABLE = "hibernate_sequences";

  /**
   * The name of the columns which holds the segment key. The segment defines the different buckets
   * (segments) of values currently tracked in the table. The default value is {@link
   * #DEFAULT_SEGMENT_COLUMN_NAME}
   */
  public static final String SEGMENT_COLUMN_NAMES_PARAM = "segment_column_names";

  /** The default {@link #SEGMENT_COLUMN_NAMES_PARAM} value */
  public static final String DEFAULT_SEGMENT_COLUMN_NAME = "sequence_name";

  /**
   * Indicates the length of the column defined by {@link #SEGMENT_COLUMN_NAMES_PARAM}. Used in
   * schema export. The default value is {@link #DEFAULT_SEGMENT_COLUMN_SIZE}
   */
  public static final String SEGMENT_COLUMN_SIZES_PARAM = "segment_column_sizes";

  /** The default {@link #SEGMENT_COLUMN_SIZES_PARAM} value */
  public static final String DEFAULT_SEGMENT_COLUMN_SIZE = "255";

  /** The name of the object field used as the segment value. */
  public static final String SEGMENT_VALUE_FIELD_NAMES_PARAM = "segment_value_field_names";

  /**
   * The name of column which holds the sequence value. The default value is {@link
   * #DEFAULT_VALUE_COLUMN}
   */
  public static final String VALUE_COLUMN_PARAM = "value_column_name";

  /** The default {@link #VALUE_COLUMN_PARAM} value */
  public static final String DEFAULT_VALUE_COLUMN = "next_val";

  /** Indicates the initial value to use. The default value is {@link #DEFAULT_INITIAL_VALUE} */
  public static final String INITIAL_PARAM = "initial_value";

  /** The default {@link #INITIAL_PARAM} value */
  public static final int DEFAULT_INITIAL_VALUE = 1;

  /** Indicates the increment size to use. The default value is {@link #DEFAULT_INCREMENT_SIZE} */
  public static final String INCREMENT_PARAM = "increment_size";

  /** The default {@link #INCREMENT_PARAM} value */
  public static final int DEFAULT_INCREMENT_SIZE = 1;

  /**
   * Indicates to enable concatenation of segment and sequence values as the generated sequence in
   * String. The default value is {@link #DEFAULT_CONCAT_SEGMENT_AND_SEQUENCE}
   */
  public static final String CONCAT_SEGMENT_AND_SEQUENCE_PARAM = "concat_segment_and_sequence";

  /** The default {@link #CONCAT_SEGMENT_AND_SEQUENCE_PARAM} value */
  private static final boolean DEFAULT_CONCAT_SEGMENT_AND_SEQUENCE = false;

  /**
   * Indicates the left padding character to be used to right-aligns the values in the generated
   * sequence. The default value is {@link #DEFAULT_LEFT_PAD_SEGMENT_AND_SEQUENCE_CHAR}
   */
  public static final String LEFT_PAD_SEGMENT_AND_SEQUENCE_CHAR_PARAM =
      "left_pad_segment_and_sequence_char";

  /** The default {@link #LEFT_PAD_SEGMENT_AND_SEQUENCE_CHAR_PARAM} value */
  private static final String DEFAULT_LEFT_PAD_SEGMENT_AND_SEQUENCE_CHAR = "0";

  /**
   * Indicates the left padding sizes of each segment and sequence values to right-aligns the values
   * in the generated sequence. This is required when {@link #CONCAT_SEGMENT_AND_SEQUENCE_PARAM} is
   * enabled.
   */
  public static final String LEFT_PAD_SEGMENT_AND_SEQUENCE_SIZES_PARAM =
      "left_pad_segment_and_sequence_sizes";

  /**
   * Indicates the select for update retry attempts when insert initial record failed caused by
   * duplicate primary keys, usually due to concurrency. The default value is {@link
   * #DEFAULT_SELECT_FOR_UPDATE_RETRY_ATTEMPTS}
   */
  public static final String SELECT_FOR_UPDATE_RETRY_ATTEMPTS_PARAM =
      "select_for_update_retry_attempts";

  /** The default {@link #SELECT_FOR_UPDATE_RETRY_ATTEMPTS_PARAM} value */
  private static final int DEFAULT_SELECT_FOR_UPDATE_RETRY_ATTEMPTS = 2;

  /**
   * Indicates the optimizer to use, either naming a {@link Optimizer} implementation class or by
   * naming a {@link StandardOptimizerDescriptor} by name
   */
  public static final String OPT_PARAM = "optimizer";

  private boolean storeLastUsedValue;

  private Type identifierType;

  private QualifiedName qualifiedTableName;
  private String renderedTableName;

  private String[] segmentColumnNames;
  private Integer[] segmentColumnSizes;
  private String[] segmentValueFieldNames;

  private String valueColumnName;
  private int initialValue;
  private int incrementSize;

  private boolean concatenateSegmentAndSequence;
  private String leftPadCharacter;
  private Integer[] leftPadSizes;

  private String selectForUpdateQuery;
  private int selectForUpdateRetryAttempts;
  private String insertQuery;
  private String updateQuery;

  private Optimizer optimizer;

  @Override
  public void configure(Type type, Properties params, ServiceRegistry serviceRegistry)
      throws MappingException {
    storeLastUsedValue =
        serviceRegistry
            .getService(ConfigurationService.class)
            .getSetting(
                AvailableSettings.TABLE_GENERATOR_STORE_LAST_USED,
                StandardConverters.BOOLEAN,
                true);
    identifierType = type;

    final JdbcEnvironment jdbcEnvironment = serviceRegistry.getService(JdbcEnvironment.class);

    qualifiedTableName = determineGeneratorTableName(params, jdbcEnvironment, serviceRegistry);

    segmentColumnNames = determineSegmentColumnNames(params, jdbcEnvironment);
    segmentColumnSizes = determineSegmentColumnSizes(params);
    segmentValueFieldNames = determineSegmentValueFieldNames(params);

    valueColumnName = determineValueColumnName(params, jdbcEnvironment);
    initialValue = determineInitialValue(params);
    incrementSize = determineIncrementSize(params);

    concatenateSegmentAndSequence = determineConcatSegmentAndSequence(params);
    leftPadCharacter = determineLeftPadCharacter(params);
    leftPadSizes = determineLeftPadSizes(params);

    selectForUpdateRetryAttempts = determineSelectForUpdateRetry(params);

    final String optimizationStrategy =
        ConfigurationHelper.getString(
            OPT_PARAM,
            params,
            OptimizerFactory.determineImplicitOptimizerName(incrementSize, params));
    int optimizerInitialValue = ConfigurationHelper.getInt(INITIAL_PARAM, params, -1);
    optimizer =
        OptimizerFactory.buildOptimizer(
            optimizationStrategy,
            identifierType.getReturnedClass(),
            incrementSize,
            optimizerInitialValue);
  }

  @Override
  public void registerExportables(Database database) {
    final Dialect dialect = database.getJdbcEnvironment().getDialect();

    final Namespace namespace =
        database.locateNamespace(
            qualifiedTableName.getCatalogName(), qualifiedTableName.getSchemaName());

    Table table = namespace.locateTable(qualifiedTableName.getObjectName());
    if (table == null) {
      table = namespace.createTable(qualifiedTableName.getObjectName(), false);
      table.setPrimaryKey(new PrimaryKey(table));

      for (int segmentColumnIndex = 0;
          segmentColumnIndex < segmentColumnNames.length;
          segmentColumnIndex++) {
        final Column segmentColumn =
            new ExportableColumn(
                database,
                table,
                segmentColumnNames[segmentColumnIndex],
                StringType.INSTANCE,
                dialect.getTypeName(Types.VARCHAR, segmentColumnSizes[segmentColumnIndex], 0, 0));
        segmentColumn.setNullable(false);
        table.addColumn(segmentColumn);
        table.getPrimaryKey().addColumn(segmentColumn);
      }

      final Column valueColumn =
          new ExportableColumn(database, table, valueColumnName, LongType.INSTANCE);
      table.addColumn(valueColumn);
    }

    // allow physical naming strategies a chance to kick in
    this.renderedTableName =
        database
            .getJdbcEnvironment()
            .getQualifiedObjectNameFormatter()
            .format(table.getQualifiedTableName(), dialect);

    this.selectForUpdateQuery = buildSelectForUpdateQuery(dialect);
    this.updateQuery = buildUpdateQuery();
    this.insertQuery = buildInsertQuery();
  }

  @Override
  public Serializable generate(SharedSessionContractImplementor session, Object object)
      throws HibernateException {
    final SqlStatementLogger statementLogger =
        session
            .getFactory()
            .getServiceRegistry()
            .getService(JdbcServices.class)
            .getSqlStatementLogger();
    final SessionEventListenerManager statsCollector = session.getEventListenerManager();

    final List<IntegralDataTypeHolder> segmentValueHolders = getSegmentValueHolders(object);

    Serializable sequence =
        optimizer.generate(
            new AccessCallback() {
              @Override
              public IntegralDataTypeHolder getNextValue() {
                return session
                    .getTransactionCoordinator()
                    .createIsolationDelegate()
                    .delegateWork(
                        new AbstractReturningWork<IntegralDataTypeHolder>() {
                          @Override
                          public IntegralDataTypeHolder execute(Connection connection)
                              throws SQLException {

                            final IntegralDataTypeHolder initialValue = prepareInitialValue();
                            boolean hasRow = false;
                            int selectForUpdateRetry = selectForUpdateRetryAttempts;
                            do {
                              hasRow =
                                  executeSelectForUpdateQuery(
                                      statementLogger,
                                      statsCollector,
                                      segmentValueHolders,
                                      connection,
                                      initialValue);
                              if (hasRow == false) {
                                try {
                                  executeInsertQuery(
                                      statementLogger,
                                      statsCollector,
                                      segmentValueHolders,
                                      connection,
                                      initialValue);
                                  hasRow = true;
                                } catch (SQLIntegrityConstraintViolationException e) {
                                  if (--selectForUpdateRetry == 0) {
                                    LOG.error(
                                        "Unable to insert into generator table '"
                                            + renderedTableName
                                            + "' with segment values '"
                                            + segmentValueHolders
                                            + "' after '"
                                            + selectForUpdateRetryAttempts
                                            + "' attempts",
                                        e);
                                    throw e;
                                  } else {
                                    LOG.trace(
                                        "Unable to insert into generator table '"
                                            + renderedTableName
                                            + "' with segment values '"
                                            + segmentValueHolders
                                            + "'",
                                        e);
                                  }
                                }
                              }
                            } while (hasRow == false);

                            executeUpdateQuery(
                                statementLogger,
                                statsCollector,
                                segmentValueHolders,
                                connection,
                                prepareIncrementedValue(initialValue));

                            if (storeLastUsedValue) {
                              return initialValue.increment();
                            } else {
                              return initialValue;
                            }
                          }
                        },
                        true);
              }

              @Override
              public String getTenantIdentifier() {
                return session.getTenantIdentifier();
              }
            });

    if (concatenateSegmentAndSequence) {
      final IntegralDataTypeHolder sequenceHolder =
          IdentifierGeneratorHelper.getIntegralDataTypeHolder(identifierType.getReturnedClass());
      sequenceHolder.initialize(toLong(sequence));
      sequence = concatenateSegmentAndSequence(segmentValueHolders, sequenceHolder);
    }
    return sequence;
  }

  private QualifiedName determineGeneratorTableName(
      Properties params, JdbcEnvironment jdbcEnvironment, ServiceRegistry serviceRegistry) {

    String fallbackTableName = DEFAULT_TABLE;

    final Boolean preferGeneratorNameAsDefaultName =
        serviceRegistry
            .getService(ConfigurationService.class)
            .getSetting(
                AvailableSettings.PREFER_GENERATOR_NAME_AS_DEFAULT_SEQUENCE_NAME,
                StandardConverters.BOOLEAN,
                true);
    if (preferGeneratorNameAsDefaultName) {
      final String generatorName = params.getProperty(IdentifierGenerator.GENERATOR_NAME);
      if (StringHelper.isNotEmpty(generatorName)) {
        fallbackTableName = generatorName;
      }
    }

    final String tableName = ConfigurationHelper.getString(TABLE_PARAM, params, fallbackTableName);

    if (tableName.contains(".")) {
      return QualifiedNameParser.INSTANCE.parse(tableName);
    } else {
      final Identifier catalog =
          jdbcEnvironment
              .getIdentifierHelper()
              .toIdentifier(ConfigurationHelper.getString(CATALOG, params));
      final Identifier schema =
          jdbcEnvironment
              .getIdentifierHelper()
              .toIdentifier(ConfigurationHelper.getString(SCHEMA, params));
      return new QualifiedNameParser.NameParts(
          catalog, schema, jdbcEnvironment.getIdentifierHelper().toIdentifier(tableName));
    }
  }

  private String[] determineSegmentColumnNames(Properties params, JdbcEnvironment jdbcEnvironment) {
    final String segmentColumnNamesString =
        ConfigurationHelper.getString(
            SEGMENT_COLUMN_NAMES_PARAM, params, DEFAULT_SEGMENT_COLUMN_NAME);
    final String[] segmentColumnNames =
        StringUtils.splitPreserveAllTokens(segmentColumnNamesString, PARAM_SEPARATOR_STRING);
    validateNotEmptyAndNoBlank(SEGMENT_COLUMN_NAMES_PARAM, segmentColumnNames);

    return Stream.of(segmentColumnNames)
        .map(
            segmentColumnName ->
                jdbcEnvironment
                    .getIdentifierHelper()
                    .toIdentifier(StringUtils.trimToEmpty(segmentColumnName))
                    .render(jdbcEnvironment.getDialect()))
        .toArray(size -> new String[size]);
  }

  private Integer[] determineSegmentColumnSizes(Properties params) {
    final String segmentColumnSizesString =
        ConfigurationHelper.getString(
            SEGMENT_COLUMN_SIZES_PARAM, params, DEFAULT_SEGMENT_COLUMN_SIZE);
    final String[] segmentColumnSizeStrings =
        StringUtils.splitPreserveAllTokens(segmentColumnSizesString, PARAM_SEPARATOR_STRING);
    validateNotEmptyAndNoBlank(SEGMENT_COLUMN_SIZES_PARAM, segmentColumnSizeStrings);
    validateNumberOfElementsEquals(
        SEGMENT_COLUMN_SIZES_PARAM, segmentColumnSizeStrings, segmentColumnNames.length);

    return Stream.of(segmentColumnSizeStrings)
        .map(segmentColumnSize -> Integer.parseUnsignedInt(StringUtils.trim(segmentColumnSize)))
        .toArray(size -> new Integer[size]);
  }

  private String[] determineSegmentValueFieldNames(Properties params) {
    final String segmentValueFieldNamesString =
        ConfigurationHelper.getString(SEGMENT_VALUE_FIELD_NAMES_PARAM, params);
    final String[] segmentValueFieldNames =
        StringUtils.splitPreserveAllTokens(segmentValueFieldNamesString, PARAM_SEPARATOR_STRING);
    validateNotEmptyAndNoBlank(SEGMENT_VALUE_FIELD_NAMES_PARAM, segmentValueFieldNames);
    validateNumberOfElementsEquals(
        SEGMENT_VALUE_FIELD_NAMES_PARAM, segmentValueFieldNames, segmentColumnNames.length);

    return Stream.of(segmentValueFieldNames)
        .map(segmentValueFieldName -> StringUtils.trimToEmpty(segmentValueFieldName))
        .toArray(size -> new String[size]);
  }

  private String determineValueColumnName(Properties params, JdbcEnvironment jdbcEnvironment) {
    final String name =
        ConfigurationHelper.getString(VALUE_COLUMN_PARAM, params, DEFAULT_VALUE_COLUMN);
    return jdbcEnvironment
        .getIdentifierHelper()
        .toIdentifier(name)
        .render(jdbcEnvironment.getDialect());
  }

  private int determineInitialValue(Properties params) {
    return ConfigurationHelper.getInt(INITIAL_PARAM, params, DEFAULT_INITIAL_VALUE);
  }

  private int determineIncrementSize(Properties params) {
    return ConfigurationHelper.getInt(INCREMENT_PARAM, params, DEFAULT_INCREMENT_SIZE);
  }

  private boolean determineConcatSegmentAndSequence(Properties params) {
    return ConfigurationHelper.getBoolean(
        CONCAT_SEGMENT_AND_SEQUENCE_PARAM, params, DEFAULT_CONCAT_SEGMENT_AND_SEQUENCE);
  }

  private String determineLeftPadCharacter(Properties params) {
    return ConfigurationHelper.getString(
        LEFT_PAD_SEGMENT_AND_SEQUENCE_CHAR_PARAM,
        params,
        DEFAULT_LEFT_PAD_SEGMENT_AND_SEQUENCE_CHAR);
  }

  private Integer[] determineLeftPadSizes(Properties params) {
    if (concatenateSegmentAndSequence == false) {
      return new Integer[0];
    }

    final String leftPadSizesString =
        ConfigurationHelper.getString(LEFT_PAD_SEGMENT_AND_SEQUENCE_SIZES_PARAM, params);
    final String[] leftPadSizeStrings =
        StringUtils.splitPreserveAllTokens(leftPadSizesString, PARAM_SEPARATOR_STRING);
    validateNotEmptyAndNoBlank(LEFT_PAD_SEGMENT_AND_SEQUENCE_SIZES_PARAM, leftPadSizeStrings);
    validateNumberOfElementsEquals(
        LEFT_PAD_SEGMENT_AND_SEQUENCE_SIZES_PARAM,
        leftPadSizeStrings,
        segmentColumnNames.length + 1);

    return Stream.of(leftPadSizeStrings)
        .map(leftPadSize -> Integer.parseUnsignedInt(StringUtils.trimToEmpty(leftPadSize)))
        .toArray(size -> new Integer[size]);
  }

  private int determineSelectForUpdateRetry(Properties params) {
    return ConfigurationHelper.getInt(
        SELECT_FOR_UPDATE_RETRY_ATTEMPTS_PARAM, params, DEFAULT_SELECT_FOR_UPDATE_RETRY_ATTEMPTS);
  }

  private void validateNotEmptyAndNoBlank(final String param, final String[] values) {
    if (ArrayUtils.isEmpty(values) || StringUtils.isAnyBlank(values)) {
      throw new IllegalStateException(
          "Parameter '" + param + "' must not be empty or contains any empty String");
    }
  }

  private void validateNumberOfElementsEquals(
      final String param, final String[] values, final int numberOfElements) {
    if (values.length != numberOfElements) {
      throw new IllegalStateException(
          "Number of elements in parameter '" + param + " must be '" + numberOfElements + "'");
    }
  }

  protected String buildSelectForUpdateQuery(Dialect dialect) {
    final String alias = "tbl";
    final StringBuilder queryBuilder =
        new StringBuilder()
            .append("select ")
            .append(StringHelper.qualify(alias, valueColumnName))
            .append(" from ")
            .append(renderedTableName)
            .append(" ")
            .append(alias)
            .append(" where ");
    final int maxSegmentColumnIndex = segmentColumnNames.length - 1;
    for (int segmentColumnIndex = 0; ; segmentColumnIndex++) {
      queryBuilder
          .append(StringHelper.qualify(alias, segmentColumnNames[segmentColumnIndex]))
          .append("=?");
      if (segmentColumnIndex == maxSegmentColumnIndex) {
        break;
      }
      queryBuilder.append(" and ");
    }
    final String query = queryBuilder.toString();

    final LockOptions lockOptions = new LockOptions(LockMode.PESSIMISTIC_WRITE);
    lockOptions.setAliasSpecificLockMode(alias, LockMode.PESSIMISTIC_WRITE);
    final Map<String, String[]> updateTargetColumnsMap =
        Collections.singletonMap(alias, new String[] {valueColumnName});
    return dialect.applyLocksToSql(query, lockOptions, updateTargetColumnsMap);
  }

  protected String buildUpdateQuery() {
    final StringBuilder queryBuilder =
        new StringBuilder()
            .append("update ")
            .append(renderedTableName)
            .append(" set ")
            .append(valueColumnName)
            .append("=?")
            .append(" where ");
    final int maxSegmentColumnIndex = segmentColumnNames.length - 1;
    for (int segmentColumnIndex = 0; ; segmentColumnIndex++) {
      queryBuilder.append(segmentColumnNames[segmentColumnIndex]).append("=?");
      if (segmentColumnIndex == maxSegmentColumnIndex) {
        break;
      }
      queryBuilder.append(" and ");
    }
    return queryBuilder.toString();
  }

  protected String buildInsertQuery() {
    final StringBuilder queryBuilder =
        new StringBuilder().append("insert into ").append(renderedTableName).append(" (");
    final int maxSegmentColumnIndex = segmentColumnNames.length - 1;
    for (int segmentColumnIndex = 0; ; segmentColumnIndex++) {
      queryBuilder.append(segmentColumnNames[segmentColumnIndex]).append(", ");
      if (segmentColumnIndex == maxSegmentColumnIndex) {
        break;
      }
    }
    queryBuilder.append(valueColumnName).append(")").append(" values (");
    for (int segmentColumnIndex = 0; ; segmentColumnIndex++) {
      queryBuilder.append("?, ");
      if (segmentColumnIndex == maxSegmentColumnIndex) {
        break;
      }
    }
    queryBuilder.append("?)");
    return queryBuilder.toString();
  }

  private IntegralDataTypeHolder prepareInitialValue() {
    long initializationValue;
    if (storeLastUsedValue) {
      initializationValue = initialValue - 1;
    } else {
      initializationValue = initialValue;
    }
    final IntegralDataTypeHolder value =
        IdentifierGeneratorHelper.getIntegralDataTypeHolder(identifierType.getReturnedClass());
    value.initialize(initializationValue);
    return value;
  }

  private IntegralDataTypeHolder prepareIncrementedValue(final IntegralDataTypeHolder value) {
    final IntegralDataTypeHolder updateValue = value.copy();
    if (optimizer.applyIncrementSizeToSourceValues()) {
      updateValue.add(incrementSize);
    } else {
      updateValue.increment();
    }
    return updateValue;
  }

  protected boolean executeSelectForUpdateQuery(
      final SqlStatementLogger statementLogger,
      final SessionEventListenerManager statsCollector,
      final List<IntegralDataTypeHolder> segmentValueHolders,
      final Connection connection,
      final IntegralDataTypeHolder initialValue)
      throws SQLException {
    try (PreparedStatement selectForUpdatePS =
        prepareStatement(connection, selectForUpdateQuery, statementLogger, statsCollector)) {
      for (int segmentColumnIndex = 0;
          segmentColumnIndex < segmentColumnNames.length;
          segmentColumnIndex++) {
        segmentValueHolders.get(segmentColumnIndex).bind(selectForUpdatePS, segmentColumnIndex + 1);
      }
      final ResultSet selectForUpdateRS = executeQuery(selectForUpdatePS, statsCollector);
      if (selectForUpdateRS.next() == false) {
        return false;
      }
      int defaultValue;
      if (storeLastUsedValue) {
        defaultValue = 0;
      } else {
        defaultValue = 1;
      }
      initialValue.initialize(selectForUpdateRS, defaultValue);
      return true;
    } catch (SQLException e) {
      LOG.unableToReadOrInitHiValue(e);
      throw e;
    }
  }

  protected void executeInsertQuery(
      final SqlStatementLogger statementLogger,
      final SessionEventListenerManager statsCollector,
      final List<IntegralDataTypeHolder> segmentValueHolders,
      final Connection connection,
      final IntegralDataTypeHolder initialValue)
      throws SQLException {
    try (PreparedStatement insertPS =
        prepareStatement(connection, insertQuery, statementLogger, statsCollector)) {
      for (int segmentColumnIndex = 0;
          segmentColumnIndex < segmentColumnNames.length;
          segmentColumnIndex++) {
        segmentValueHolders.get(segmentColumnIndex).bind(insertPS, segmentColumnIndex + 1);
      }

      initialValue.bind(insertPS, segmentColumnNames.length + 1);
      executeUpdate(insertPS, statsCollector);
    }
  }

  protected int executeUpdateQuery(
      final SqlStatementLogger statementLogger,
      final SessionEventListenerManager statsCollector,
      final List<IntegralDataTypeHolder> segmentValueHolders,
      final Connection connection,
      final IntegralDataTypeHolder updateValue)
      throws SQLException {
    try (PreparedStatement updatePS =
        prepareStatement(connection, updateQuery, statementLogger, statsCollector)) {
      updateValue.bind(updatePS, 1);

      for (int segmentColumnIndex = 0;
          segmentColumnIndex < segmentColumnNames.length;
          segmentColumnIndex++) {
        segmentValueHolders.get(segmentColumnIndex).bind(updatePS, segmentColumnIndex + 2);
      }
      return executeUpdate(updatePS, statsCollector);
    } catch (SQLException e) {
      LOG.unableToUpdateQueryHiValue(renderedTableName, e);
      throw e;
    }
  }

  private PreparedStatement prepareStatement(
      Connection connection,
      String sql,
      SqlStatementLogger statementLogger,
      SessionEventListenerManager statsCollector)
      throws SQLException {
    statementLogger.logStatement(sql, FormatStyle.BASIC.getFormatter());
    try {
      statsCollector.jdbcPrepareStatementStart();
      return connection.prepareStatement(sql);
    } finally {
      statsCollector.jdbcPrepareStatementEnd();
    }
  }

  private int executeUpdate(PreparedStatement ps, SessionEventListenerManager statsCollector)
      throws SQLException {
    try {
      statsCollector.jdbcExecuteStatementStart();
      return ps.executeUpdate();
    } finally {
      statsCollector.jdbcExecuteStatementEnd();
    }
  }

  private ResultSet executeQuery(PreparedStatement ps, SessionEventListenerManager statsCollector)
      throws SQLException {
    try {
      statsCollector.jdbcExecuteStatementStart();
      return ps.executeQuery();
    } finally {
      statsCollector.jdbcExecuteStatementEnd();
    }
  }

  private List<IntegralDataTypeHolder> getSegmentValueHolders(final Object object) {
    return Stream.of(segmentValueFieldNames)
        .map(
            (segmentFieldName) -> {
              try {
                return FieldUtils.getField(object.getClass(), segmentFieldName, true);
              } catch (Exception e) {
                throw new IllegalStateException(
                    "Unable to get segment field '"
                        + segmentFieldName
                        + "' from type '"
                        + object.getClass()
                        + "'",
                    e);
              }
            })
        .map(
            (segmentField) -> {
              final IntegralDataTypeHolder valueHolder =
                  IdentifierGeneratorHelper.getIntegralDataTypeHolder(segmentField.getType());
              try {
                valueHolder.initialize(toLong(segmentField.get(object)));
              } catch (Exception e) {
                throw new IllegalStateException(
                    "Unable to get segment value from field '"
                        + segmentField.getName()
                        + "' from type '"
                        + object.getClass()
                        + "'",
                    e);
              }
              return valueHolder;
            })
        .collect(Collectors.toList());
  }

  private Number concatenateSegmentAndSequence(
      final List<IntegralDataTypeHolder> segmentValueHolders,
      final IntegralDataTypeHolder sequenceHolder) {

    final List<IntegralDataTypeHolder> valueHolders =
        new ArrayList<IntegralDataTypeHolder>(segmentValueHolders);
    valueHolders.add(sequenceHolder);

    final StringBuilder sequenceBuilder = new StringBuilder();
    for (int index = 0; index < leftPadSizes.length; index++) {
      sequenceBuilder.append(
          StringUtils.leftPad(
              toString(valueHolders.get(index).makeValue()),
              leftPadSizes[index],
              leftPadCharacter));
    }

    final IntegralDataTypeHolder resultHolder =
        IdentifierGeneratorHelper.getIntegralDataTypeHolder(identifierType.getReturnedClass());
    resultHolder.initialize(Long.valueOf(sequenceBuilder.toString()));
    return resultHolder.makeValue();
  }

  private String toString(final Number number) {
    final Class<?> numberType = number.getClass();
    if (numberType == BigDecimal.class) {
      return ((BigDecimal) number).toPlainString();
    } else if (numberType == BigInteger.class) {
      return ((BigInteger) number).toString();
    } else if (numberType == Long.class
        || numberType == Integer.class
        || numberType == Short.class) {
      return number.toString();
    } else {
      throw new IdentifierGenerationException(
          "Unknown integral type '" + numberType.getName() + "'");
    }
  }

  private long toLong(final Object object) {
    final Class<?> numberType = object.getClass();
    if (numberType == BigDecimal.class) {
      return ((BigDecimal) object).longValue();
    } else if (numberType == BigInteger.class) {
      return ((BigInteger) object).longValue();
    } else if (numberType == Long.class
        || numberType == Integer.class
        || numberType == Short.class) {
      return (long) object;
    } else {
      throw new IdentifierGenerationException(
          "Unknown integral type '" + numberType.getName() + "'");
    }
  }

  @Override
  public String[] sqlCreateStrings(Dialect dialect) throws HibernateException {
    final StringBuilder queryBuilder =
        new StringBuilder()
            .append(dialect.getCreateTableString())
            .append(" ")
            .append(renderedTableName)
            .append(" ( ");
    final int maxSegmentColumnIndex = segmentColumnNames.length - 1;
    for (int segmentColumnIndex = 0; ; segmentColumnIndex++) {
      queryBuilder
          .append(segmentColumnNames[segmentColumnIndex])
          .append(" ")
          .append(dialect.getTypeName(Types.VARCHAR, segmentColumnSizes[segmentColumnIndex], 0, 0))
          .append(" not null ");
      if (segmentColumnIndex == maxSegmentColumnIndex) {
        break;
      }
      queryBuilder.append(", ");
    }
    queryBuilder
        .append(valueColumnName)
        .append(" ")
        .append(dialect.getTypeName(Types.BIGINT))
        .append(", primary key ( ");
    for (int segmentColumnIndex = 0; ; segmentColumnIndex++) {
      queryBuilder.append(segmentColumnNames[segmentColumnIndex]);
      if (segmentColumnIndex == maxSegmentColumnIndex) {
        break;
      }
      queryBuilder.append(", ");
    }
    queryBuilder.append(" ) )").append(dialect.getTableTypeString());

    return new String[] {queryBuilder.toString()};
  }

  @Override
  public String[] sqlDropStrings(Dialect dialect) throws HibernateException {
    return new String[] {dialect.getDropTableString(renderedTableName)};
  }

  @Override
  public Object generatorKey() {
    return qualifiedTableName.render();
  }
}
