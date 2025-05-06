import json
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Discounts(Stream):
    """Stream class for Shopify Discounts."""
    name = "discounts"
    data_key = "discountNodes"
    replication_key = "updatedAt"
    
    def transform_object(self, obj: dict):
        """
        Transforms a discount object.

        Args:
            obj (dict): Discount object.

        Returns:
            dict: Transformed discount object.
        """
        
        discount = obj.pop ("discount")
        
        obj["discount"] = json.dumps(discount)
        obj["createdAt"] = discount.get("createdAt")
        obj["updatedAt"] = discount.get("updatedAt")

        return obj

    def get_query(self):
        """
        Returns the GraphQL query for fetching discounts.

        Returns:
            str: GraphQL query string.
        """
        return """
            query Discounts($first: Int!, $after: String, $query: String) {
                discountNodes(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                    edges {
                        node {
                            id
                            discount {
                                # DiscountCodeBasic
                                ... on DiscountCodeBasic {
                                    __typename
                                    appliesOncePerCustomer
                                    asyncUsageCount
                                    codes(first: 250) {
                                        edges {
                                            node {
                                                code
                                            }
                                        }
                                    }
                                    codesCount {
                                        count
                                        precision
                                    }
                                    combinesWith {
                                        orderDiscounts
                                        productDiscounts
                                        shippingDiscounts
                                    }
                                    createdAt
                                    customerGets {
                                        items {
                                            __typename
                                            ... on AllDiscountItems {
                                                allItems
                                            }
                                            ... on DiscountProducts {
                                                products(first: 250) {
                                                edges {
                                                    node {
                                                        id
                                                        title
                                                        }
                                                    }
                                                }
                                            }
                                            ... on DiscountCollections {
                                                collections(first: 250) {
                                                edges {
                                                    node {
                                                            id
                                                            title
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        value {
                                        ... on DiscountPercentage {
                                            percentage
                                        }
                                        ... on DiscountAmount {
                                                amount {
                                                    amount
                                                    currencyCode
                                                }
                                            }
                                        }
                                    }
                                    customerSelection {
                                        __typename
                                        ... on DiscountCustomerAll {
                                            allCustomers
                                        }
                                        ... on DiscountCustomers {
                                            customers {
                                                id
                                            }
                                        }
                                        ... on DiscountCustomerSegments {
                                            segments {
                                                id
                                            }
                                        }
                                    }
                                    endsAt
                                    hasTimelineComment
                                    minimumRequirement {
                                        __typename
                                        ... on DiscountMinimumQuantity {
                                            greaterThanOrEqualToQuantity
                                        }
                                        ... on DiscountMinimumSubtotal {
                                            greaterThanOrEqualToSubtotal {
                                                amount
                                                currencyCode
                                            }
                                        }
                                    }
                                    recurringCycleLimit
                                    shareableUrls {
                                        url
                                    }
                                    shortSummary
                                    startsAt
                                    status
                                    summary
                                    title
                                    totalSales {
                                        amount
                                        currencyCode
                                    }
                                    updatedAt
                                    usageLimit
                                }
                                # DiscountCodeBxgy
                                ... on DiscountCodeBxgy {
                                    __typename
                                    appliesOncePerCustomer
                                    asyncUsageCount
                                    codes(first: 250) {
                                        edges {
                                            node {
                                                code
                                            }
                                        }
                                    }
                                    codesCount {
                                        count
                                        precision
                                    }
                                    combinesWith {
                                        orderDiscounts
                                        productDiscounts
                                        shippingDiscounts
                                    }
                                    createdAt
                                    customerBuys {
                                        isOneTimePurchase
                                        isSubscription
                                        items {
                                            __typename
                                            ... on AllDiscountItems {
                                                allItems
                                            }
                                            ... on DiscountCollections {
                                                collections(first: 250) {
                                                    edges {
                                                        node {
                                                            id
                                                            title
                                                        }
                                                    }
                                                }
                                            }
                                            ... on DiscountProducts {
                                                products(first: 250) {
                                                    edges {
                                                        node {
                                                            id
                                                            title
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        value {
                                            __typename
                                            ... on DiscountQuantity {
                                                quantity
                                            }
                                            ... on DiscountPurchaseAmount {
                                                amount
                                            }
                                        }
                                    }
                                    customerGets {
                                        appliesOnOneTimePurchase
                                        appliesOnSubscription
                                        items {
                                            __typename
                                            ... on AllDiscountItems {
                                                allItems
                                            }
                                            ... on DiscountProducts {
                                                products(first: 250) {
                                                edges {
                                                    node {
                                                        id
                                                        title
                                                        }
                                                    }
                                                }
                                            }
                                            ... on DiscountCollections {
                                                collections(first: 250) {
                                                edges {
                                                    node {
                                                            id
                                                            title
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        value {
                                        ... on DiscountPercentage {
                                            percentage
                                        }
                                        ... on DiscountAmount {
                                            amount {
                                                    amount
                                                    currencyCode
                                                }
                                            }
                                        }
                                    }
                                    customerSelection {
                                        __typename
                                        ... on DiscountCustomerAll {
                                            allCustomers
                                        }
                                        ... on DiscountCustomers {
                                            customers {
                                                id
                                            }
                                        }
                                        ... on DiscountCustomerSegments {
                                            segments {
                                                id
                                            }
                                        }
                                    }
                                    endsAt
                                    hasTimelineComment
                                    shareableUrls {
                                        url
                                    }
                                    startsAt
                                    status
                                    summary
                                    title
                                    totalSales {
                                        amount
                                        currencyCode
                                    }
                                    updatedAt
                                    usageLimit
                                    usesPerOrderLimit
                                }
                                # DiscountCodeFreeShipping
                                ... on DiscountCodeFreeShipping {
                                    __typename
                                    appliesOncePerCustomer
                                    appliesOnOneTimePurchase
                                    appliesOnSubscription
                                    asyncUsageCount
                                    codes(first: 250) {
                                        edges {
                                            node {
                                                code
                                            }
                                        }
                                    }
                                    codesCount {
                                        count
                                        precision
                                    }
                                    combinesWith {
                                        orderDiscounts
                                        productDiscounts
                                        shippingDiscounts
                                    }
                                    createdAt
                                    customerSelection {
                                        __typename
                                        ... on DiscountCustomerAll {
                                            allCustomers
                                        }
                                        ... on DiscountCustomers {
                                            customers {
                                                id
                                            }
                                        }
                                        ... on DiscountCustomerSegments {
                                            segments {
                                                id
                                            }
                                        }
                                    }
                                    destinationSelection {
                                        __typename
                                        ... on DiscountCountries {
                                            countries
                                            includeRestOfWorld
                                        }
                                        ... on DiscountCountryAll {
                                            allCountries
                                        }
                                    }
                                    endsAt
                                    hasTimelineComment
                                    maximumShippingPrice {
                                        amount
                                        currencyCode
                                    }
                                    minimumRequirement {
                                        __typename
                                        ... on DiscountMinimumQuantity {
                                            greaterThanOrEqualToQuantity
                                        }
                                        ... on DiscountMinimumSubtotal {
                                            greaterThanOrEqualToSubtotal {
                                                amount
                                                currencyCode
                                            }
                                        }
                                    }
                                    recurringCycleLimit
                                    shareableUrls {
                                        url
                                    }
                                    shortSummary
                                    startsAt
                                    status
                                    summary
                                    title
                                    totalSales {
                                        amount
                                        currencyCode
                                    }
                                    updatedAt
                                    usageLimit
                                }
                                # DiscountAutomaticBasic
                                ... on DiscountAutomaticBasic {
                                    __typename
                                    asyncUsageCount
                                    combinesWith {
                                        orderDiscounts
                                        productDiscounts
                                        shippingDiscounts
                                    }
                                    createdAt
                                    customerGets {
                                        items {
                                            __typename
                                            ... on AllDiscountItems {
                                                allItems
                                            }
                                            ... on DiscountProducts {
                                                products(first: 250) {
                                                edges {
                                                    node {
                                                    id
                                                    title
                                                    }
                                                }
                                                }
                                            }
                                            ... on DiscountCollections {
                                                collections(first: 250) {
                                                edges {
                                                    node {
                                                            id
                                                            title
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        value {
                                            __typename
                                        ... on DiscountPercentage {
                                                percentage
                                            }
                                            ... on DiscountAmount {
                                                amount {
                                                    amount
                                                    currencyCode
                                                }
                                            }
                                        }
                                    }
                                    endsAt
                                    minimumRequirement {
                                        __typename
                                        ... on DiscountMinimumQuantity {
                                            greaterThanOrEqualToQuantity
                                        }
                                        ... on DiscountMinimumSubtotal {
                                            greaterThanOrEqualToSubtotal {
                                                amount
                                                currencyCode
                                            }
                                        }
                                    }
                                    recurringCycleLimit
                                    shortSummary
                                    startsAt
                                    status
                                    summary
                                    title
                                    updatedAt
                                }
                                # DiscountAutomaticBxgy
                                ... on DiscountAutomaticBxgy {
                                    __typename
                                    asyncUsageCount
                                    combinesWith {
                                        orderDiscounts
                                        productDiscounts
                                        shippingDiscounts
                                    }
                                    createdAt
                                    customerBuys {
                                        isOneTimePurchase
                                        isSubscription
                                        items {
                                            __typename
                                            ... on AllDiscountItems {
                                                allItems
                                            }
                                            ... on DiscountCollections {
                                                collections(first: 250) {
                                                    edges {
                                                        node {
                                                            id
                                                            title
                                                        }
                                                    }
                                                }
                                            }
                                            ... on DiscountProducts {
                                                products(first: 250) {
                                                    edges {
                                                        node {
                                                            id
                                                            title
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        value {
                                            __typename
                                            ... on DiscountQuantity {
                                                quantity
                                            }
                                            ... on DiscountPurchaseAmount {
                                                amount
                                            }
                                        }
                                    }
                                    customerGets {
                                        appliesOnOneTimePurchase
                                        appliesOnSubscription
                                        items {
                                            __typename
                                            ... on AllDiscountItems {
                                                allItems
                                            }
                                            ... on DiscountProducts {
                                                products(first: 250) {
                                                edges {
                                                    node {
                                                            id
                                                            title
                                                        }
                                                    }
                                                }
                                            }
                                            ... on DiscountCollections {
                                                collections(first: 250) {
                                                edges {
                                                    node {
                                                            id
                                                            title
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        value {
                                            __typename
                                            ... on DiscountPercentage {
                                                percentage
                                            }
                                            ... on DiscountAmount {
                                                amount {
                                                    amount
                                                    currencyCode
                                                }
                                            }
                                        }
                                    }
                                    endsAt
                                    startsAt
                                    status
                                    summary
                                    title
                                    updatedAt
                                    usesPerOrderLimit
                                }
                                # DiscountAutomaticFreeShipping
                                ... on DiscountAutomaticFreeShipping {
                                    __typename
                                    appliesOnOneTimePurchase
                                    appliesOnSubscription
                                    asyncUsageCount
                                    combinesWith {
                                        orderDiscounts
                                        productDiscounts
                                        shippingDiscounts
                                    }
                                    createdAt
                                    destinationSelection {
                                        __typename
                                        ... on DiscountCountries {
                                            countries
                                            includeRestOfWorld
                                        }
                                        ... on DiscountCountryAll {
                                            allCountries
                                        }
                                    }
                                    endsAt
                                    hasTimelineComment
                                    maximumShippingPrice {
                                        amount
                                        currencyCode
                                    }
                                    minimumRequirement {
                                        __typename
                                        ... on DiscountMinimumQuantity {
                                            greaterThanOrEqualToQuantity
                                        }
                                        ... on DiscountMinimumSubtotal {
                                            greaterThanOrEqualToSubtotal {
                                                amount
                                                currencyCode
                                            }
                                        }
                                    }
                                    recurringCycleLimit
                                    shortSummary
                                    startsAt
                                    status
                                    summary
                                    title
                                    totalSales {
                                        amount
                                        currencyCode
                                    }
                                    updatedAt
                                }
                                # DiscountAutomaticApp
                                ... on DiscountAutomaticApp {
                                    appDiscountType {
                                        functionId
                                    }
                                    appliesOnOneTimePurchase
                                    appliesOnSubscription
                                    asyncUsageCount
                                    combinesWith {
                                        orderDiscounts
                                        productDiscounts
                                        shippingDiscounts
                                    }
                                    createdAt
                                    discountClass
                                    discountId
                                    endsAt
                                    errorHistory {
                                        errorsFirstOccurredAt
                                        firstOccurredAt
                                        hasBeenSharedSinceLastError
                                        hasSharedRecentErrors
                                    }
                                    recurringCycleLimit
                                    startsAt
                                    status
                                    title
                                    updatedAt
                                }
                                # DiscountCodeApp
                                ... on DiscountCodeApp {
                                    appDiscountType {
                                        functionId
                                    }
                                    appliesOncePerCustomer
                                    appliesOnOneTimePurchase
                                    appliesOnSubscription
                                    asyncUsageCount
                                    codes(first: 250) {
                                        edges {
                                            node {
                                                code
                                            }
                                        }
                                    }
                                    codesCount {
                                        count
                                        precision
                                    }
                                    combinesWith {
                                        orderDiscounts
                                        productDiscounts
                                        shippingDiscounts
                                    }
                                    createdAt
                                    customerSelection {
                                        __typename
                                        ... on DiscountCustomerAll {
                                            allCustomers
                                        }
                                        ... on DiscountCustomers {
                                            customers {
                                                id
                                            }
                                        }
                                        ... on DiscountCustomerSegments {
                                            segments {
                                                id
                                            }
                                        }
                                    }
                                    discountClass
                                    discountId
                                    endsAt
                                    hasTimelineComment
                                    recurringCycleLimit
                                    shareableUrls {
                                        url
                                    }
                                    startsAt
                                    status
                                    title
                                    totalSales {
                                        amount
                                        currencyCode
                                    }
                                    updatedAt
                                    usageLimit
                                }
                            }
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        """

Context.stream_objects["discounts"] = Discounts
