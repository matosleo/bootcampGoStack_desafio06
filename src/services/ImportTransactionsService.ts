import csvParse from 'csv-parse';
import fs from 'fs';
import { getRepository, getCustomRepository, In } from 'typeorm';
import Transaction from '../models/Transaction';
import Category from '../models/Category';
import TransactionsRepository from '../repositories/TransactionsRepository';

interface FileTransaction {
  title: string;
  type: 'income' | 'outcome';
  value: number;
  category: string;
}

class ImportTransactionsService {
  async execute(filePath: string): Promise<Transaction[]> {
    const categoriesRepository = getRepository(Category);
    const transactionsRepository = getCustomRepository(TransactionsRepository);

    const readFileStream = fs.createReadStream(filePath);

    const parseStream = csvParse({
      from_line: 2,
    });

    const parseFile = readFileStream.pipe(parseStream);

    const categories: string[] = [];
    const transactions: FileTransaction[] = [];

    parseFile.on('data', async line => {
      const [title, type, value, category] = line.map((cell: string) =>
        cell.trim(),
      );

      if (!title || !type || !value) return;

      categories.push(category);
      transactions.push({ title, type, value, category });
    });

    await new Promise(resolve => {
      parseFile.on('end', resolve);
    });

    const existentCategories = await categoriesRepository.find({
      where: { title: In(categories) },
    });

    const existentCategoriestitles = existentCategories.map(
      (category: Category) => category.title,
    );

    const addCategoriesTitles = categories
      .filter(category => !existentCategoriestitles.includes(category))
      .filter((value, index, self) => self.indexOf(value) === index);

    const newCategories = categoriesRepository.create(
      addCategoriesTitles.map(title => ({
        title,
      })),
    );

    await categoriesRepository.save(newCategories);

    const finalCategories = [...newCategories, ...existentCategories];

    const createdTransactions = transactionsRepository.create(
      transactions.map(transaction => ({
        title: transaction.title,
        type: transaction.type,
        value: transaction.value,
        category: finalCategories.find(
          category => category.title === transaction.category,
        ),
      })),
    );

    await transactionsRepository.save(createdTransactions);

    await fs.promises.unlink(filePath);

    return createdTransactions;
  }
}

export default ImportTransactionsService;
