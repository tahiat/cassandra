/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.primitives.Keys;
import accord.primitives.Txn;
import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.StatementSource;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.transactions.ConditionStatement;
import org.apache.cassandra.cql3.transactions.ReferenceOperation;
import org.apache.cassandra.cql3.transactions.RowDataReference;
import org.apache.cassandra.cql3.transactions.SelectReferenceSource;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnDataName;
import org.apache.cassandra.service.accord.txn.TxnNamedRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnReference;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Collectors3;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class TransactionStatement implements CQLStatement.CompositeCQLStatement, CQLStatement.ReturningCQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionStatement.class);

    public static final String DUPLICATE_TUPLE_NAME_MESSAGE = "The name '%s' has already been used by a LET assignment.";
    public static final String INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE = "SELECT must specify either all primary key elements or all partition key elements and LIMIT 1. In both cases partition key elements must be always specified with equality operators; %s %s";
    public static final String NO_CONDITIONS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify their own conditions; %s statement %s";
    public static final String NO_TIMESTAMPS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify custom timestamps; %s statement %s";
    public static final String EMPTY_TRANSACTION_MESSAGE = "Transaction contains no reads or writes";
    public static final String SELECT_REFS_NEED_COLUMN_MESSAGE = "SELECT references must specify a column.";
    public static final String TRANSACTIONS_DISABLED_MESSAGE = "Accord transactions are disabled. (See accord_transactions_enabled in cassandra.yaml)";
    public static final String ILLEGAL_RANGE_QUERY_MESSAGE = "Range queries are not allowed for reads within a transaction; %s %s";
    public static final String SELECT_ALREADY_DEFINED_MESSAGE = "Unconditional SELECT statement has been already defined for the transaction; %s";
    public static final String MISSING_SELECT_IN_BRANCH_MESSAGE = "SELECT statement is missing in branch; %s";
    public static final String MISSING_DEFAULT_BRANCH_MESSAGE = "SELECT statement is missing in ELSE branch; %s. If there is no ELSE branch, it has to be added and SELECT statement has to be defined there";
    public static final String CONDITIONAL_BLOCKS_HAVE_INCONSISTENT_RESULT_SETS_MESSAGE = "Conditional blocks %s and %s have inconsistent result sets: %s != %s";
    public static final String CONDITIONAL_BLOCKS_HAVE_INCONSISTENT_RETURNING_CLAUSES_MESSAGE = "Conditional blocks %s and %s have inconsistent returning clauses";
    public static final String CANNOT_SPECIFY_BOTH_SELECT_AND_LET_REFS_MESSAGE = "Cannot specify both a full SELECT and a SELECT w/ LET references";

    static class NamedSelect
    {
        final TxnDataName name;
        final SelectStatement select;

        public NamedSelect(TxnDataName name, SelectStatement select)
        {
            this.name = name;
            this.select = select;
        }
    }

    private final @Nonnull List<NamedSelect> assignments;
    private final NamedSelect returningSelect;
    private final @Nonnull List<RowDataReference> returningReferences;
    private final @Nonnull List<ModificationStatement> updates;
    private final @Nonnull List<ConditionStatement> conditions;

    private final VariableSpecifications bindVariables;
    private final ResultSet.ResultMetadata resultMetadata;

    public TransactionStatement(List<NamedSelect> assignments,
                                NamedSelect returningSelect,
                                List<RowDataReference> returningReferences,
                                List<ModificationStatement> updates,
                                List<ConditionStatement> conditions,
                                VariableSpecifications bindVariables)
    {
        this.assignments = assignments;
        this.returningSelect = returningSelect;
        this.returningReferences = returningReferences;
        this.updates = updates;
        this.conditions = conditions;
        this.bindVariables = bindVariables;

        if (returningSelect != null)
        {
            resultMetadata = returningSelect.select.getResultMetadata();
        }
        else if (returningReferences != null && !returningReferences.isEmpty())
        {
            List<ColumnSpecification> names = new ArrayList<>(returningReferences.size());
            for (RowDataReference reference : returningReferences)
                names.add(reference.toResultMetadata());
            resultMetadata = new ResultSet.ResultMetadata(names);
        }
        else
        {
            resultMetadata =  ResultSet.ResultMetadata.EMPTY;
        }
    }

    public List<ModificationStatement> getUpdates()
    {
        return updates;
    }

    @Override
    public List<ColumnSpecification> getBindVariables()
    {
        return bindVariables.getBindVariables();
    }

    @Override
    public void authorize(ClientState state)
    {
        // Assess read permissions for all data from both explicit LET statements and generated reads.
        for (NamedSelect let : assignments)
            let.select.authorize(state);

        if (returningSelect != null)
            returningSelect.select.authorize(state);

        for (ModificationStatement update : updates)
            update.authorize(state);
    }

    @Override
    public void validate(ClientState state)
    {
        for (NamedSelect statement : assignments)
            statement.select.validate(state);
        if (returningSelect != null)
            returningSelect.select.validate(state);
        for (ModificationStatement statement : updates)
            statement.validate(state);
    }

    @Override
    public Iterable<CQLStatement> getStatements()
    {
        return () -> {
            Stream<CQLStatement> stream = assignments.stream().map(n -> n.select);
            if (returningSelect != null)
                stream = Stream.concat(stream, Stream.of(returningSelect.select));
            stream = Stream.concat(stream, updates.stream());
            return stream.iterator();
        };
    }

    @Override
    public ResultSet.ResultMetadata getResultMetadata()
    {
        return resultMetadata;
    }

    TxnNamedRead createNamedRead(NamedSelect namedSelect, QueryOptions options, ClientState state)
    {
        SelectStatement select = namedSelect.select;
        // We reject reads from both LET and SELECT that do not specify a single row.
        @SuppressWarnings("unchecked")
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) select.getQuery(options, 0);

        if (selectQuery.queries.size() != 1)
            throw new IllegalArgumentException("Within a transaction, SELECT statements must select a single partition; found " + selectQuery.queries.size() + " partitions");

        return new TxnNamedRead(namedSelect.name, Iterables.getOnlyElement(selectQuery.queries));
    }

    List<TxnNamedRead> createNamedReads(NamedSelect namedSelect, QueryOptions options, ClientState state)
    {
        SelectStatement select = namedSelect.select;
        // We reject reads from both LET and SELECT that do not specify a single row.
        @SuppressWarnings("unchecked")
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) select.getQuery(options, 0);

        if (selectQuery.queries.size() == 1)
            return Collections.singletonList(new TxnNamedRead(namedSelect.name, Iterables.getOnlyElement(selectQuery.queries)));

        List<TxnNamedRead> list = new ArrayList<>(selectQuery.queries.size());
        for (int i = 0; i < selectQuery.queries.size(); i++)
            list.add(new TxnNamedRead(TxnDataName.returning(i), selectQuery.queries.get(i)));
        return list;
    }

    private List<TxnNamedRead> createNamedReads(QueryOptions options, ClientState state, Map<TxnDataName, NamedSelect> autoReads, Consumer<Key> keyConsumer)
    {
        List<TxnNamedRead> reads = new ArrayList<>(assignments.size() + 1);

        for (NamedSelect select : assignments)
        {
            TxnNamedRead read = createNamedRead(select, options, state);
            keyConsumer.accept(read.key());
            reads.add(read);
        }

        if (returningSelect != null)
        {
            for (TxnNamedRead read : createNamedReads(returningSelect, options, state))
            {
                keyConsumer.accept(read.key());
                reads.add(read);
            }
        }

        for (NamedSelect select : autoReads.values())
            // don't need keyConsumer as the keys are known to exist due to Modification
            reads.add(createNamedRead(select, options, state));

        return reads;
    }

    TxnCondition createCondition(QueryOptions options)
    {
        if (conditions.isEmpty())
            return TxnCondition.none();
        if (conditions.size() == 1)
            return conditions.get(0).createCondition(options);

        List<TxnCondition> result = new ArrayList<>(conditions.size());
        for (ConditionStatement condition : conditions)
            result.add(condition.createCondition(options));

        // TODO: OR support
        return new TxnCondition.BooleanGroup(TxnCondition.Kind.AND, result);
    }

    List<TxnWrite.Fragment> createWriteFragments(ClientState state, QueryOptions options, Map<TxnDataName, NamedSelect> autoReads, Consumer<Key> keyConsumer)
    {
        List<TxnWrite.Fragment> fragments = new ArrayList<>(updates.size());
        int idx = 0;
        for (ModificationStatement modification : updates)
        {
            TxnWrite.Fragment fragment = modification.getTxnWriteFragment(idx, state, options);
            keyConsumer.accept(fragment.key);
            fragments.add(fragment);

            if (modification.allReferenceOperations().stream().anyMatch(ReferenceOperation::requiresRead))
            {
                // Reads are not merged by partition here due to potentially differing columns retrieved, etc.
                TxnDataName partitionName = TxnDataName.partitionRead(modification.metadata(), fragment.key.partitionKey(), idx);
                if (!autoReads.containsKey(partitionName))
                    autoReads.put(partitionName, new NamedSelect(partitionName, modification.createSelectForTxn()));
            }

            idx++;
        }
        return fragments;
    }

    TxnUpdate createUpdate(ClientState state, QueryOptions options, Map<TxnDataName, NamedSelect> autoReads, Consumer<Key> keyConsumer)
    {
        return new TxnUpdate(createWriteFragments(state, options, autoReads, keyConsumer), createCondition(options));
    }

    Keys toKeys(SortedSet<Key> keySet)
    {
        return new Keys(keySet);
    }

    @VisibleForTesting
    public Txn createTxn(ClientState state, QueryOptions options)
    {
        SortedSet<Key> keySet = new TreeSet<>();

        if (updates.isEmpty())
        {
            // TODO: Test case around this...
            Preconditions.checkState(conditions.isEmpty(), "No condition should exist without updates present");
            List<TxnNamedRead> reads = createNamedReads(options, state, ImmutableMap.of(), keySet::add);
            Keys txnKeys = toKeys(keySet);
            TxnRead read = new TxnRead(reads, txnKeys);
            return new Txn.InMemory(txnKeys, read, TxnQuery.ALL);
        }
        else
        {
            Map<TxnDataName, NamedSelect> autoReads = new HashMap<>();
            TxnUpdate update = createUpdate(state, options, autoReads, keySet::add);
            List<TxnNamedRead> reads = createNamedReads(options, state, autoReads, keySet::add);
            Keys txnKeys = toKeys(keySet);
            TxnRead read = new TxnRead(reads, txnKeys);
            return new Txn.InMemory(txnKeys, read, TxnQuery.ALL, update);
        }
    }

    /**
     * Returns {@code true} only if the statement selects multiple clusterings in a partition
     */
    private static boolean isSelectingMultipleClusterings(SelectStatement select, @Nullable QueryOptions options)
    {
        if (select.getRestrictions().hasAllPrimaryKeyColumnsRestrictedByEqualities())
            return false;

        if (options == null)
        {
            // if the limit is a non-terminal marker (because we're preparing), defer validation until execution (when options != null)
            if (select.isLimitMarker())
                return false;

            options = QueryOptions.DEFAULT;
        }

        return select.getLimit(options) != 1;
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        checkTrue(DatabaseDescriptor.getAccordTransactionsEnabled(), TRANSACTIONS_DISABLED_MESSAGE);

        try
        {
            // check again since now we have query options; note that statements are quaranted to be single partition reads at this point
            for (NamedSelect assignment : assignments)
                checkFalse(isSelectingMultipleClusterings(assignment.select, options), INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, "LET assignment", assignment.select.source);

            if (returningSelect != null)
                checkFalse(isSelectingMultipleClusterings(returningSelect.select, options), INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, "returning SELECT", returningSelect.select.source);

            TxnData data = AccordService.instance().coordinate(createTxn(state.getClientState(), options), options.getConsistency());

            if (returningSelect != null)
            {
                @SuppressWarnings("unchecked")
                SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) returningSelect.select.getQuery(options, 0);
                Selection.Selectors selectors = returningSelect.select.getSelection().newSelectors(options);
                ResultSetBuilder result = new ResultSetBuilder(resultMetadata, selectors, null);
                if (selectQuery.queries.size() == 1)
                {
                    FilteredPartition partition = data.get(TxnDataName.returning());
                    if (partition != null)
                        returningSelect.select.processPartition(partition.rowIterator(), options, result, FBUtilities.nowInSeconds());
                }
                else
                {
                    int nowInSec = FBUtilities.nowInSeconds();
                    for (int i = 0; i < selectQuery.queries.size(); i++)
                    {
                        FilteredPartition partition = data.get(TxnDataName.returning(i));
                        if (partition != null)
                            returningSelect.select.processPartition(partition.rowIterator(), options, result, nowInSec);
                    }
                }
                return new ResultMessage.Rows(result.build());
            }

            if (!returningReferences.isEmpty())
            {
                List<AbstractType<?>> resultType = new ArrayList<>(returningReferences.size());
                List<ColumnMetadata> columns = new ArrayList<>(returningReferences.size());

                for (RowDataReference reference : returningReferences)
                {
                    ColumnMetadata forMetadata = reference.toResultMetadata();
                    resultType.add(forMetadata.type);
                    columns.add(reference.column());
                }

                ResultSetBuilder result = new ResultSetBuilder(resultMetadata, Selection.noopSelector(), null);
                result.newRow(options.getProtocolVersion(), null, null, columns);

                for (int i = 0; i < returningReferences.size(); i++)
                {
                    RowDataReference reference = returningReferences.get(i);
                    TxnReference txnReference = reference.toTxnReference(options);
                    ByteBuffer buffer = txnReference.toByteBuffer(data, resultType.get(i));
                    result.add(buffer);
                }

                return new ResultMessage.Rows(result.build());
            }

            // In the case of a write-only transaction, just return and empty result.
            // TODO: This could be modified to return an indication of whether a condition (if present) succeeds.
            return new ResultMessage.Void();
        }
        catch (Throwable t)
        {
            //TODO remove before merge to trunk
           logger.error("Unexpected error with transaction: {}", t.toString());
           throw t;
        }
    }

    @Override
    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        return execute(state, options, nanoTime());
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.TRANSACTION);
    }

    public static class Parsed extends QualifiedStatement.Composite
    {
        private final @Nonnull List<SelectStatement.RawStatement> assignments;
        private final @Nullable SelectStatement.RawStatement select;
        private final @Nonnull List<RowDataReference.Raw> returning;
        private final @Nonnull List<ModificationStatement.Parsed> unconditionalUpdates;
        private final @Nonnull List<ConditionalBlock.Raw> blocks;
        private final @Nonnull List<RowDataReference.Raw> dataReferences;

        public Parsed(List<SelectStatement.RawStatement> assignments,
                      SelectStatement.RawStatement select,
                      List<RowDataReference.Raw> returning,
                      List<ModificationStatement.Parsed> unconditionalUpdates,
                      List<ConditionalBlock.Raw> blocks,
                      List<RowDataReference.Raw> dataReferences)
        {
            this.assignments = assignments != null ? assignments : ImmutableList.of();
            this.select = select;
            this.returning = returning != null ? returning : ImmutableList.of();
            this.unconditionalUpdates = unconditionalUpdates != null ? unconditionalUpdates : ImmutableList.of();
            this.blocks = blocks != null ? blocks : ImmutableList.of();
            this.dataReferences = dataReferences != null ? dataReferences : ImmutableList.of();
        }

        @Override
        protected Iterable<? extends QualifiedStatement> getStatements()
        {
            Iterable<? extends QualifiedStatement> group = Iterables.concat(assignments, unconditionalUpdates);
            if (select != null)
                group = Iterables.concat(group, Collections.singleton(select));
            for (ConditionalBlock.Raw block : blocks)
                group = Iterables.concat(group, block.getStatements());
            return group;
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            checkFalse(unconditionalUpdates.isEmpty() && returning.isEmpty() && select == null && blocks.isEmpty(), EMPTY_TRANSACTION_MESSAGE);
            checkFalse(select != null && !returning.isEmpty(), CANNOT_SPECIFY_BOTH_SELECT_AND_LET_REFS_MESSAGE, select != null ? select.source : null);

            List<ConditionalBlock.Raw> conditionalBlocks = getConditionalBlocks();

            List<NamedSelect> preparedAssignments = new ArrayList<>(assignments.size());
            Map<TxnDataName, RowDataReference.ReferenceSource> refSources = new HashMap<>();
            Set<TxnDataName> selectNames = new HashSet<>();

            for (SelectStatement.RawStatement select : assignments)
            {
                checkNotNull(select.parameters.refName, "Assignments must be named");
                TxnDataName name = TxnDataName.user(select.parameters.refName);
                checkTrue(selectNames.add(name), DUPLICATE_TUPLE_NAME_MESSAGE, name.name());

                SelectStatement prepared = select.prepare(bindVariables);
                NamedSelect namedSelect = new NamedSelect(name, prepared);
                checkAtMostOneRowSpecified(namedSelect.select, "LET assignment " + name.name());
                preparedAssignments.add(namedSelect);
                refSources.put(name, new SelectReferenceSource(prepared));
            }

            for (RowDataReference.Raw reference : dataReferences)
                reference.resolveReference(refSources);

            NamedSelect returningSelect = null;
            if (select != null)
            {
                returningSelect = new NamedSelect(TxnDataName.returning(), select.prepare(bindVariables));
                checkAtMostOneRowSpecified(returningSelect.select, "returning select");
            }

            ImmutableList<ConditionalBlock> preparedBlocks = conditionalBlocks.stream()
                                                                              .map(b -> b.prepare(state, bindVariables))
                                                                              .collect(Collectors3.toImmutableList());

            preparedBlocks.forEach(b -> b.validateSelection(preparedBlocks.get(0)));

            return new TransactionStatement(preparedAssignments, returningSelect, preparedBlocks.get(0).returningReferences, preparedBlocks.get(0).updates, preparedBlocks.get(0).conditions , bindVariables);
        }

        private List<ConditionalBlock.Raw> getConditionalBlocks()
        {
            if (blocks.isEmpty())
            {
                StatementSource source = unconditionalUpdates.isEmpty() ? null : unconditionalUpdates.get(0).source;
                return ImmutableList.of(new ConditionalBlock.Raw(returning, unconditionalUpdates, null, source));
            }
            else
            {
                assert unconditionalUpdates.isEmpty(); // should be enforced by parser

                ImmutableList.Builder<ConditionalBlock.Raw> rebuiltBlocks = ImmutableList.builder();
                boolean blockMustHaveReturning = blocks.stream().anyMatch(ConditionalBlock.Raw::hasReturning);
                boolean blockMayHaveReturning = select == null && returning.isEmpty();

                for (ConditionalBlock.Raw b : blocks)
                {
                    checkFalse(!blockMayHaveReturning && b.hasReturning(), SELECT_ALREADY_DEFINED_MESSAGE, b.source);
                    checkFalse(blockMustHaveReturning && !b.hasReturning(), b.conditions.isEmpty() ? MISSING_DEFAULT_BRANCH_MESSAGE : MISSING_SELECT_IN_BRANCH_MESSAGE, b.source);
                    rebuiltBlocks.add(!returning.isEmpty() && !b.hasReturning() ? b.withReturning(returning) : b);
                }

                return rebuiltBlocks.build();
            }
        }
    }

    public static class ConditionalBlock
    {
        private final @Nonnull List<ConditionStatement> conditions;
        private final @Nonnull List<RowDataReference> returningReferences;
        private final @Nonnull List<ModificationStatement> updates;
        private final StatementSource source;

        public ConditionalBlock(List<ConditionStatement> conditions,
                                List<RowDataReference> returningReferences,
                                List<ModificationStatement> updates,
                                StatementSource source)
        {
            this.conditions = conditions != null ? conditions : ImmutableList.of();
            this.returningReferences = returningReferences != null ? returningReferences : ImmutableList.of();
            this.updates = updates != null ? updates : ImmutableList.of();
            this.source = source;
        }

        public boolean isDefault()
        {
            return conditions.isEmpty();
        }

        public void validateSelection(ConditionalBlock referenceBlock)
        {
            checkTrue((returningReferences.isEmpty()) == (referenceBlock.returningReferences.isEmpty()), CONDITIONAL_BLOCKS_HAVE_INCONSISTENT_RETURNING_CLAUSES_MESSAGE, source, referenceBlock.source);

            if (!returningReferences.isEmpty())
            {
                List<AbstractType<?>> thisTypes = returningReferences.stream().map(r -> r.toResultMetadata().type).collect(Collectors.toList());
                List<AbstractType<?>> refTypes = referenceBlock.returningReferences.stream().map(r -> r.toResultMetadata().type).collect(Collectors.toList());
                checkTrue(thisTypes.equals(refTypes), String.format(CONDITIONAL_BLOCKS_HAVE_INCONSISTENT_RESULT_SETS_MESSAGE, source, referenceBlock.source, thisTypes, refTypes));
            }
        }

        public static class Raw
        {
            public final @Nonnull List<RowDataReference.Raw> returning;
            public final @Nonnull List<ModificationStatement.Parsed> updates;
            public final @Nonnull List<ConditionStatement.Raw> conditions;
            public final @Nullable StatementSource source;

            public Raw(List<RowDataReference.Raw> returning,
                       List<ModificationStatement.Parsed> updates,
                       List<ConditionStatement.Raw> conditions,
                       StatementSource source)
            {
                this.returning = returning == null ? Collections.emptyList() : returning;
                this.updates = updates == null ? Collections.emptyList() : updates;
                this.conditions = conditions == null ? Collections.emptyList() : conditions;
                this.source = source;
            }

            public ConditionalBlock prepare(ClientState state, VariableSpecifications bindVariables)
            {
                List<RowDataReference> returningReferences = null;
                if (!returning.isEmpty())
                {
                    // TODO: Eliminate/modify this check if we allow full tuple selections.
                    returningReferences = returning.stream().peek(raw -> checkTrue(raw.column() != null, SELECT_REFS_NEED_COLUMN_MESSAGE))
                                                   .map(RowDataReference.Raw::prepareAsReceiver)
                                                   .collect(Collectors.toList());
                }

                List<ModificationStatement> preparedUpdates = new ArrayList<>(updates.size());

                // check for any read-before-write updates
                for (int i = 0; i < updates.size(); i++)
                {
                    ModificationStatement.Parsed parsed = updates.get(i);

                    ModificationStatement prepared = parsed.prepare(state, bindVariables);
                    checkFalse(prepared.hasConditions(), NO_CONDITIONS_IN_UPDATES_MESSAGE, prepared.type, prepared.source);
                    checkFalse(prepared.isTimestampSet(), NO_TIMESTAMPS_IN_UPDATES_MESSAGE, prepared.type, prepared.source);

                    preparedUpdates.add(prepared);
                }

                List<ConditionStatement> preparedConditions = new ArrayList<>(conditions.size());
                for (ConditionStatement.Raw condition : conditions)
                    // TODO: If we eventually support IF ks.function(ref) THEN, the keyspace will have to be provided here
                    preparedConditions.add(condition.prepare("[txn]", bindVariables));

                return new ConditionalBlock(preparedConditions, returningReferences, preparedUpdates, source);
            }

            public Iterable<? extends QualifiedStatement> getStatements()
            {
                return updates;
            }

            public boolean hasReturning()
            {
                return !returning.isEmpty();
            }

            public Raw withReturning(List<RowDataReference.Raw> returning)
            {
                return new Raw(returning, updates, conditions, source);
            }

            public boolean isEmpty()
            {
                return !hasReturning() && updates.isEmpty();
            }
        }

    }

    /**
     * Do not use this method in execution!!! It is only allowed during prepare because it outputs a query raw text.
     * We don't want it print it for a user who provided an identifier of someone's else prepared statement.
     */
    private static void checkAtMostOneRowSpecified(SelectStatement select, String name)
    {
        checkFalse(select.isPartitionRangeQuery(), ILLEGAL_RANGE_QUERY_MESSAGE, name, select.source);
        checkFalse(isSelectingMultipleClusterings(select, null), INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, name, select.source);
    }
}
