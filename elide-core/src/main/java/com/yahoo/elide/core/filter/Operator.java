/*
 * Copyright 2015, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.core.filter;

import com.yahoo.elide.core.Path;
import com.yahoo.elide.core.PersistentResource;
import com.yahoo.elide.core.RequestScope;
import com.yahoo.elide.core.exceptions.BadRequestException;
import com.yahoo.elide.core.exceptions.InvalidOperatorNegationException;
import com.yahoo.elide.utils.coerce.CoerceUtil;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Operator enum for predicates.
 */
@RequiredArgsConstructor
public enum Operator {
    IN("in", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return in(fieldPath, values, requestScope);
        }
    },

    IN_INSENSITIVE("ini", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return in(fieldPath, values, requestScope, FOLD_CASE);
        }
    },

    NOT("not", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return (T entity) -> !in(fieldPath, values, requestScope).test(entity);
        }
    },

    NOT_INSENSITIVE("noti", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return (T entity) -> !in(fieldPath, values, requestScope, FOLD_CASE).test(entity);
        }
    },

    PREFIX_CASE_INSENSITIVE("prefixi", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return prefix(fieldPath, values, requestScope, s -> s.toLowerCase(Locale.ENGLISH));
        }
    },

    PREFIX("prefix", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return prefix(fieldPath, values, requestScope, Function.identity());
        }
    },

    POSTFIX("postfix", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return postfix(fieldPath, values, requestScope, Function.identity());
        }
    },

    POSTFIX_CASE_INSENSITIVE("postfixi", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return postfix(fieldPath, values, requestScope, FOLD_CASE);
        }
    },

    INFIX("infix", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return infix(fieldPath, values, requestScope, Function.identity());
        }
    },

    INFIX_CASE_INSENSITIVE("infixi", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return infix(fieldPath, values, requestScope, FOLD_CASE);
        }
    },

    ISNULL("isnull", false) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return isNull(fieldPath, requestScope);
        }
    },

    NOTNULL("notnull", false) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return (val) -> !isNull(fieldPath, requestScope).test(val);
        }
    },

    LT("lt", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return lt(fieldPath, values, requestScope);
        }
    },

    LE("le", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return le(fieldPath, values, requestScope);
        }
    },

    GT("gt", true) {
        @Override
        public <T> Predicate<T> contextualize(
                Path fieldPath, List<Object> values, RequestScope requestScope) {
            return gt(fieldPath, values, requestScope);
        }
    },

    GE("ge", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return ge(fieldPath, values, requestScope);
        }
    },

    TRUE("true", false) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return isTrue();
        }
    },

    FALSE("false", false) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return isFalse();
        }
    },

    ISEMPTY("isempty", false) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return isEmpty(fieldPath, requestScope);
        }
    },

    NOTEMPTY("notempty", false) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return (entity) -> !isEmpty(fieldPath, requestScope).test(entity);
        }
    },

    HASMEMBER("hasmember", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return hasMember(fieldPath, values, requestScope);
        }
    },

    HASNOMEMBER("hasnomember", true) {
        @Override
        public <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope) {
            return entiry -> !hasMember(fieldPath, values, requestScope).test(entiry);
        }
    }
    ;

    public static final Function<String, String> FOLD_CASE = s -> s.toLowerCase(Locale.ENGLISH);
    @Getter private final String notation;
    @Getter private final boolean parameterized;
    private Operator negated;

    // initialize negated values
    static {
        GE.negated = LT;
        GT.negated = LE;
        LE.negated = GT;
        LT.negated = GE;
        IN.negated = NOT;
        IN_INSENSITIVE.negated = NOT_INSENSITIVE;
        NOT.negated = IN;
        NOT_INSENSITIVE.negated = IN_INSENSITIVE;
        TRUE.negated = FALSE;
        FALSE.negated = TRUE;
        ISNULL.negated = NOTNULL;
        NOTNULL.negated = ISNULL;
        ISEMPTY.negated = NOTEMPTY;
        NOTEMPTY.negated = ISEMPTY;
        HASMEMBER.negated = HASNOMEMBER;
        HASNOMEMBER.negated = HASMEMBER;
    }

    /**
     * Returns Operator from query parameter operator notation.
     *
     * @param string operator notation from query parameter
     * @return Operator
     */
    public static Operator fromString(final String string) {
        for (final Operator operator : values()) {
            if (operator.getNotation().equals(string)) {
                return operator;
            }
        }

        throw new BadRequestException("Unknown operator in filter: " + string);
    }

    public abstract <T> Predicate<T> contextualize(Path fieldPath, List<Object> values, RequestScope requestScope);

    //
    // Predicate generation
    //

    //
    // In with strict equality
    private static <T> Predicate<T> in(Path fieldPath, List<Object> values, RequestScope requestScope) {
        return (T entity) -> {
            Object val = getFieldValue(entity, fieldPath, requestScope);

            return val != null && values.stream()
                    .map(v -> CoerceUtil.coerce(v, val.getClass()))
                    .anyMatch(val::equals);
        };
    }

    //
    // String-like In with optional transformation
    private static <T> Predicate<T> in(Path fieldPath, List<Object> values,
                                       RequestScope requestScope, Function<String, String> transform) {
        return (T entity) -> {
            Object fieldValue = getFieldValue(entity, fieldPath, requestScope);

            if (fieldValue == null) {
                return false;
            }

            if (!fieldValue.getClass().isAssignableFrom(String.class)) {
                throw new IllegalStateException("Cannot case insensitive compare non-string values");
            }

            String val = transform.apply((String) fieldValue);
            return val != null && values.stream()
                    .map(v -> transform.apply(CoerceUtil.coerce(v, String.class)))
                    .anyMatch(val::equals);
        };
    }

    //
    // String-like prefix matching with optional transformation
    private static <T> Predicate<T> prefix(Path fieldPath, List<Object> values,
                                           RequestScope requestScope, Function<String, String> transform) {
        return (T entity) -> {
            if (values.size() != 1) {
                throw new BadRequestException("PREFIX can only take one argument");
            }

            Object val = getFieldValue(entity, fieldPath, requestScope);
            String valStr = CoerceUtil.coerce(val, String.class);
            String filterStr = CoerceUtil.coerce(values.get(0), String.class);

            return valStr != null
                    && filterStr != null
                    && transform.apply(valStr).startsWith(transform.apply(filterStr));
        };
    }

    //
    // String-like postfix matching with optional transformation
    private static <T> Predicate<T> postfix(Path fieldPath, List<Object> values,
                                            RequestScope requestScope, Function<String, String> transform) {
        return (T entity) -> {
            if (values.size() != 1) {
                throw new BadRequestException("POSTFIX can only take one argument");
            }

            Object val = getFieldValue(entity, fieldPath, requestScope);
            String valStr = CoerceUtil.coerce(val, String.class);
            String filterStr = CoerceUtil.coerce(values.get(0), String.class);

            return valStr != null
                    && filterStr != null
                    && transform.apply(valStr).endsWith(transform.apply(filterStr));
        };
    }

    //
    // String-like infix matching with optional transformation
    private static <T> Predicate<T> infix(Path fieldPath, List<Object> values,
                                          RequestScope requestScope, Function<String, String> transform) {
        return (T entity) -> {
            if (values.size() != 1) {
                throw new BadRequestException("INFIX can only take one argument");
            }

            Object val = getFieldValue(entity, fieldPath, requestScope);
            String valStr = CoerceUtil.coerce(val, String.class);
            String filterStr = CoerceUtil.coerce(values.get(0), String.class);

            return valStr != null
                    && filterStr != null
                    && transform.apply(valStr).contains(transform.apply(filterStr));
        };
    }

    //
    // Null checking
    private static <T> Predicate<T> isNull(Path fieldPath, RequestScope requestScope) {
        return (T entity) -> getFieldValue(entity, fieldPath, requestScope) == null;
    }

    private static <T> Predicate<T> lt(Path fieldPath, List<Object> values, RequestScope requestScope) {
        return getComparator(fieldPath, values, requestScope, compareResult -> compareResult < 0);
    }

    private static <T> Predicate<T> le(Path fieldPath, List<Object> values, RequestScope requestScope) {
        return getComparator(fieldPath, values, requestScope, compareResult -> compareResult <= 0);
    }

    private static <T> Predicate<T> gt(Path fieldPath, List<Object> values, RequestScope requestScope) {
        return getComparator(fieldPath, values, requestScope, compareResult -> compareResult > 0);
    }

    private static <T> Predicate<T> ge(Path fieldPath, List<Object> values, RequestScope requestScope) {
        return getComparator(fieldPath, values, requestScope, compareResult -> compareResult >= 0);
    }

    private static <T> Predicate<T> isTrue() {
        return (T entity) -> true;
    }

    private static <T> Predicate<T> isFalse() {
        return (T entity) -> false;
    }

    private static <T> Predicate<T> isEmpty(Path fieldPath, RequestScope requestScope) {
        return (T entity) -> {

            Object val = getFieldValue(entity, fieldPath, requestScope);
            if (val == null) { return false; }
            if (val instanceof Collection<?>) {
                return ((Collection<?>) val).isEmpty();
            }
            if (val instanceof Map<?, ?>) {
                return ((Map<?, ?>) val).isEmpty();
            }

            return false;
        };
    }

    private static <T> Predicate<T> hasMember(Path fieldPath, List<Object> values, RequestScope requestScope) {
        return (T entity) -> {
            if (values.size() != 1) {
                throw new BadRequestException("HasMember can only take one argument");
            }
            Object val = getFieldValue(entity, fieldPath, requestScope);
            Object filterStr = fieldPath.lastElement()
                    .map(last -> CoerceUtil.coerce(values.get(0), last.getFieldType()))
                    .orElse(CoerceUtil.coerce(values.get(0), String.class));

            if (val == null) { return false; }
            if (val instanceof Collection<?>) {
                return ((Collection<?>) val).contains(filterStr);
            }
            if (val instanceof Map<?, ?>) {
                return ((Map<?, ?>) val).keySet().contains(filterStr);
            }

            return false;
        };
    }

    /**
     * Return value of field/path for given entity.  For example this.book.author
     *
     * @param <T> the type of entity to retrieve a value from
     * @param entity Entity bean
     * @param fieldPath field value/path
     * @param requestScope Request scope
     * @return the value of the field
     */
    private static <T> Object getFieldValue(T entity, Path fieldPath, RequestScope requestScope) {
        Object val = entity;
        for (Path.PathElement field : fieldPath.getPathElements()) {
            if ("this".equals(field)) {
                continue;
            }
            if (val == null) {
                break;
            }
            val = PersistentResource.getValue(val, field.getFieldName(), requestScope);
        }
        return val;
    }

    private static <T> Predicate<T> getComparator(Path fieldPath, List<Object> values,
                                                  RequestScope requestScope, Predicate<Integer> condition) {
        return (T entity) -> {
            if (values.size() == 0) {
                throw new BadRequestException("No value to compare");
            }
            Object fieldVal = getFieldValue(entity, fieldPath, requestScope);
            return fieldVal != null
                    && values.stream()
                    .anyMatch(testVal -> condition.test(compare(fieldVal, testVal)));
        };

    }

    private static int compare(Object fieldValue, Object rawTestValue) {
        Object testValue = CoerceUtil.coerce(rawTestValue, fieldValue.getClass());
        Comparable testComp = CoerceUtil.coerce(testValue, Comparable.class);
        Comparable fieldComp = CoerceUtil.coerce(fieldValue, Comparable.class);

        return fieldComp.compareTo(testComp);
    }

    public Operator negate() {
        if (negated == null) {
            throw new InvalidOperatorNegationException();
        }
        return negated;
    }
}
