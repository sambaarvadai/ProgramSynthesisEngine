import type { SchemaConfig } from '../compiler/schema/schema-config.js';

export const crmSchema: SchemaConfig = {
  version: '1.0.0',
  description: 'CRM database with customers, orders, products, and support tickets',
  tables: new Map([
    [
      'customers',
      {
        name: 'customers',
        description: 'Customer accounts and their subscription details',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            description: 'Unique customer ID',
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false,
            description: 'Company or person name',
          },
          {
            name: 'email',
            type: { kind: 'string' },
            nullable: false,
            description: 'Billing email',
          },
          {
            name: 'segment',
            type: { kind: 'string' },
            nullable: false,
            description: 'enterprise, smb, or startup',
          },
          {
            name: 'region',
            type: { kind: 'string' },
            nullable: false,
            description: 'us-east, us-west, eu-west, ap-south',
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            description: 'Account creation date',
          },
          {
            name: 'arr',
            type: { kind: 'number' },
            nullable: true,
            description: 'Annual recurring revenue in USD',
          },
        ],
      },
    ],
    [
      'products',
      {
        name: 'products',
        description: 'Products and subscription plans available for purchase',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            description: 'Unique product ID',
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false,
            description: 'Product name',
          },
          {
            name: 'category',
            type: { kind: 'string' },
            nullable: false,
            description: 'subscription, add-on, storage, support, service',
          },
          {
            name: 'price',
            type: { kind: 'number' },
            nullable: false,
            description: 'Unit price in USD',
          },
          {
            name: 'is_active',
            type: { kind: 'boolean' },
            nullable: false,
            description: 'Whether product is currently sold',
          },
        ],
      },
    ],
    [
      'orders',
      {
        name: 'orders',
        description: 'Customer purchase orders',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            description: 'Unique order ID',
          },
          {
            name: 'customer_id',
            type: { kind: 'number' },
            nullable: false,
            description: 'References customers.id',
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            description: 'pending, processing, completed, cancelled, refunded',
          },
          {
            name: 'total',
            type: { kind: 'number' },
            nullable: false,
            description: 'Order total in USD',
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            description: 'When order was placed',
          },
          {
            name: 'completed_at',
            type: { kind: 'datetime' },
            nullable: true,
            description: 'When order was fulfilled, null if pending',
          },
        ],
      },
    ],
    [
      'order_items',
      {
        name: 'order_items',
        description: 'Individual line items within an order',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            description: 'Unique line item ID',
          },
          {
            name: 'order_id',
            type: { kind: 'number' },
            nullable: false,
            description: 'References orders.id',
          },
          {
            name: 'product_id',
            type: { kind: 'number' },
            nullable: false,
            description: 'References products.id',
          },
          {
            name: 'quantity',
            type: { kind: 'number' },
            nullable: false,
            description: 'Number of units',
          },
          {
            name: 'unit_price',
            type: { kind: 'number' },
            nullable: false,
            description: 'Price per unit at time of order',
          },
        ],
      },
    ],
    [
      'support_tickets',
      {
        name: 'support_tickets',
        description: 'Customer support requests and their resolution status',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            description: 'Unique ticket ID',
          },
          {
            name: 'customer_id',
            type: { kind: 'number' },
            nullable: false,
            description: 'References customers.id',
          },
          {
            name: 'subject',
            type: { kind: 'string' },
            nullable: false,
            description: 'Ticket subject line',
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            description: 'open, in_progress, resolved, closed',
          },
          {
            name: 'priority',
            type: { kind: 'string' },
            nullable: false,
            description: 'low, medium, high, critical',
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            description: 'When ticket was opened',
          },
          {
            name: 'resolved_at',
            type: { kind: 'datetime' },
            nullable: true,
            description: 'When resolved, null if still open',
          },
        ],
      },
    ],
  ]),
  foreignKeys: [
    {
      fromTable: 'orders',
      fromColumn: 'customer_id',
      toTable: 'customers',
      toColumn: 'id',
      description: 'Order belongs to customer',
    },
    {
      fromTable: 'order_items',
      fromColumn: 'order_id',
      toTable: 'orders',
      toColumn: 'id',
      description: 'Line item belongs to order',
    },
    {
      fromTable: 'order_items',
      fromColumn: 'product_id',
      toTable: 'products',
      toColumn: 'id',
      description: 'Line item references product',
    },
    {
      fromTable: 'support_tickets',
      fromColumn: 'customer_id',
      toTable: 'customers',
      toColumn: 'id',
      description: 'Ticket belongs to customer',
    },
  ],
};
