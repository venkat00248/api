import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { ApolloServer } from '@apollo/server';
import { expressMiddleware } from '@apollo/server/express4';
import mongoose from 'mongoose';
import { Kafka } from 'kafkajs';

type Product = {
  id: string;
  name: string;
  description: string;
  price: number;
  category: string;
  imageUrl: string;
  inventory: number;
};

const KAFKA_TOPICS = {
  orderCreated: 'order-created'
} as const;

const products: Product[] = [
  { id: 'p-1', name: 'Everyday Carry Tote', description: 'A structured canvas tote for work and weekends.', price: 68, category: 'Bags', imageUrl: 'https://images.unsplash.com/photo-1542291026-7eec264c27ff?auto=format&fit=crop&w=900&q=80', inventory: 12 },
  { id: 'p-2', name: 'Studio Headphones', description: 'Warm sound, all-day comfort, no distractions.', price: 129, category: 'Audio', imageUrl: 'https://images.unsplash.com/photo-1505740420928-5e560c06d30e?auto=format&fit=crop&w=900&q=80', inventory: 7 },
  { id: 'p-3', name: 'Ceramic Pour Over', description: 'Slow mornings, better coffee, beautifully made.', price: 42, category: 'Home', imageUrl: 'https://images.unsplash.com/photo-1517256064527-09c73fc73e38?auto=format&fit=crop&w=900&q=80', inventory: 19 }
];

const typeDefs = `#graphql
  type Product { id: ID!, name: String!, description: String!, price: Float!, category: String!, imageUrl: String!, inventory: Int! }
  enum ProductSort { PRICE_ASC PRICE_DESC NAME_ASC }
  type ProductPage { items: [Product!]!, total: Int!, page: Int!, pageSize: Int! }
  type Query {
    products(search: String, category: String, minPrice: Float, maxPrice: Float, sort: ProductSort, page: Int = 1, pageSize: Int = 12): ProductPage!
    product(id: ID!): Product
    categories: [String!]!
  }
`;
type ProductQueryArgs = { search?: string; category?: string; minPrice?: number; maxPrice?: number; sort?: 'PRICE_ASC' | 'PRICE_DESC' | 'NAME_ASC'; page?: number; pageSize?: number };
const resolvers = {
  Query: {
    products: (_: unknown, args: ProductQueryArgs) => {
      const page = Math.max(args.page ?? 1, 1);
      const pageSize = Math.min(Math.max(args.pageSize ?? 12, 1), 50);
      const search = args.search?.trim().toLowerCase();
      const filtered = products.filter((product) => {
        const matchesSearch = !search || `${product.name} ${product.description} ${product.category}`.toLowerCase().includes(search);
        const matchesCategory = !args.category || product.category.toLowerCase() === args.category.toLowerCase();
        const matchesMin = args.minPrice === undefined || product.price >= args.minPrice;
        const matchesMax = args.maxPrice === undefined || product.price <= args.maxPrice;
        return matchesSearch && matchesCategory && matchesMin && matchesMax;
      });
      const sorted = [...filtered].sort((first, second) => {
        if (args.sort === 'PRICE_ASC') return first.price - second.price;
        if (args.sort === 'PRICE_DESC') return second.price - first.price;
        if (args.sort === 'NAME_ASC') return first.name.localeCompare(second.name);
        return 0;
      });
      const start = (page - 1) * pageSize;
      return { items: sorted.slice(start, start + pageSize), total: sorted.length, page, pageSize };
    },
    product: (_: unknown, { id }: { id: string }) => products.find((product) => product.id === id),
    categories: () => [...new Set(products.map((product) => product.category))].sort()
  }
};
const port = Number(process.env.PORT ?? 4000);

async function start() {
  const app = express();
  const apollo = new ApolloServer({ typeDefs, resolvers });
  await apollo.start();
  app.use(cors(), express.json());
  app.get('/health', (_request, response) => response.json({ status: 'ok', service: 'api' }));
  app.get('/1', (_request, response) => response.json({ status: 'ok from dev', service: 'api from dev' }));
  app.use('/graphql', expressMiddleware(apollo));
  app.listen(port, '0.0.0.0', () => console.log(`API listening on http://0.0.0.0:${port}/graphql`));
  if (process.env.MONGO_URI) await mongoose.connect(process.env.MONGO_URI).catch((error) => console.warn('Mongo unavailable:', error.message));
  if (process.env.KAFKA_BROKERS) { new Kafka({ clientId: 'ecommerce-api', brokers: process.env.KAFKA_BROKERS.split(',') }); console.log(`Kafka topic ready: ${KAFKA_TOPICS.orderCreated}`); }
}
start().catch((error) => { console.error(error); process.exit(1); });