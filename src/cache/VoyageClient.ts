export class VoyageClient {
  private apiKey: string;
  private model: string;
  private baseUrl = 'https://api.voyageai.com/v1';

  constructor(apiKey: string, model = 'voyage-3') {
    this.apiKey = apiKey;
    this.model = model;
  }

  async embed(text: string): Promise<number[]> {
    const response = await fetch(`${this.baseUrl}/embeddings`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.apiKey}`
      },
      body: JSON.stringify({
        model: this.model,
        input: [text],
        input_type: 'query'
      })
    });

    if (!response.ok) {
      const err = await response.text();
      throw new Error(`Voyage API error: ${response.status} ${err}`);
    }

    const data = await response.json();
    return data.data[0].embedding as number[];
  }

  async embedBatch(texts: string[]): Promise<number[][]> {
    const response = await fetch(`${this.baseUrl}/embeddings`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.apiKey}`
      },
      body: JSON.stringify({
        model: this.model,
        input: texts,
        input_type: 'query'
      })
    });

    if (!response.ok) {
      const err = await response.text();
      throw new Error(`Voyage batch embed error: ${response.status} ${err}`);
    }

    const data = await response.json();
    return data.data.map((d: any) => d.embedding) as number[][];
  }
}
