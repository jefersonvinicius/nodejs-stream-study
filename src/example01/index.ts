// This example the stream is be created, transformed and write into a file

import { pipeline, Readable, Transform } from 'stream';
import { randomUUID } from 'crypto';
import faker from 'faker';
import { createWriteStream } from 'fs';
import path from 'path';

const readableStream = new Readable({
  read: function () {
    for (let index = 0; index < 1e2; index++) {
      const data = JSON.stringify(createPerson());
      this.push(data);
    }

    this.push(null);
  },
});

const addAgeProperty = new Transform({
  transform: (chunk, _, callback) => {
    const person = JSON.parse(chunk.toString());
    person.age = calculateAge(new Date(person.birthDate));
    const newChunk = JSON.stringify(person);
    callback(null, newChunk);
  },
});

const transformToCSV = new Transform({
  transform: (chunk, _, callback) => {
    const person = JSON.parse(chunk.toString());
    const row = `${person.id},${person.name},${person.email},${person.age},${person.birthDate}\n`;
    callback(null, row);
  },
});

const filePath = path.join(__dirname, './people.csv');
const finalFile = createWriteStream(filePath);
finalFile.write('id,name,email,age,birth_date\n');

pipeline(readableStream, addAgeProperty, transformToCSV, finalFile, (err) => {
  console.log('Pipeline Ended');
  if (err) {
    console.log('Error: ', err?.message);
  }
});

function createPerson() {
  const name = faker.name.findName();
  return {
    id: randomUUID(),
    name: name,
    email: faker.internet.email(name),
    birthDate: faker.date.past(10, new Date(2020, 0, 1)),
  };
}

const YEAR_IN_MILLISECONDS = 1000 * 60 * 60 * 24 * 30 * 12;
function calculateAge(birthDate: Date) {
  const now = Date.now();
  const birthDateMs = birthDate.getTime();
  const diffInMs = now - birthDateMs;
  return Math.floor(diffInMs / YEAR_IN_MILLISECONDS);
}
