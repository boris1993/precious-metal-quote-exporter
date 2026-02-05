import { InfluxDBClient, Point } from '@influxdata/influxdb3-client';

// CloudFlare worker only recognizes https URLs for WebSocket connections
const WEBSOCKET_URL = 'https://api.itick.org/forex';

export class ITickWebSocketManager implements DurableObject {
	private retryCount = 0;
	private maxRetries = 5;

	private readonly iTickApiToken: string;
	private readonly influxDBClient: InfluxDBClient;

	private activeWebSocket?: WebSocket;

	constructor(
		private state: DurableObjectState,
		private env: Env
	) {
		if (!env.ITICK_API_TOKEN) {
			throw new Error('ITICK_API_TOKEN is not defined in environment variables');
		}

		if (!env.INFLUX_DB_HOST || !env.INFLUX_DB_TOKEN || !env.INFLUX_DB_BUCKET) {
			throw new Error('INFLUX_DB_HOST, INFLUX_DB_TOKEN or INFLUX_DB_BUCKET is not defined in environment variables');
		}

		this.iTickApiToken = env.ITICK_API_TOKEN;
		this.influxDBClient = new InfluxDBClient({
			host: env.INFLUX_DB_HOST,
			token: env.INFLUX_DB_TOKEN,
			database: env.INFLUX_DB_BUCKET
		});

		this.state.blockConcurrencyWhile(async () => {
			await this.connectWithRetry();
		});
	}

	async fetch(request: Request): Promise<Response> {
		return new Response('ITick WebSocket Manager Durable Object is running.');
	}

	async alarm(): Promise<void> {
		console.log('Alarm triggered, checking connection / heartbeat');
		if (this.activeWebSocket && this.activeWebSocket.readyState === WebSocket.OPEN) {
			const pingMessage = JSON.stringify({
				ac: 'ping',
				params: Date.now().toString(),
			});

			try {
				this.activeWebSocket.send(pingMessage);
			} catch (error) {
				console.error('Failed to send heartbeat:', error);
				this.handleReconnect();
			}
		} else {
			console.log('WebSocket not connected, attempting to reconnect.');
			await this.connectWithRetry();
		}

		await this.state.storage.setAlarm(Date.now() + 30000);
	}

	private async connectWithRetry(): Promise<void> {
		if (this.activeWebSocket && this.activeWebSocket.readyState === WebSocket.OPEN) {
			// Skip re-creating the connection when a valid one exists
			return;
		}

		try {
			console.log(`Attempting to connect to WebSocket. Attempt=${this.retryCount + 1}`);

			const response = await fetch(WEBSOCKET_URL, {
				headers: {
					'Upgrade': 'websocket',
					'token': this.iTickApiToken,
				},
			});

			const wsConnection = response.webSocket;
			if (!wsConnection) {
				throw new Error('WebSocket connection failed');
			}

			wsConnection.accept();
			this.activeWebSocket = wsConnection;
			this.retryCount = 0;
			await this.state.storage.setAlarm(Date.now() + 30000);

			wsConnection.addEventListener('close', async () => {
				console.log('WebSocket connection closed. Reconnecting...');
				await this.connectWithRetry();
			});

			wsConnection.addEventListener('message', async (msg) => {
				await this.handleResponse(msg);
			});
		} catch (error) {
			console.error('WebSocket connection error:', error);
			await this.handleReconnect();
		}
	}

	private async handleResponse(message: MessageEvent) {
		const data = JSON.parse(message.data);
		if (data.resAc) {
			// Handles responses to specific actions
			const resAc = data.resAc;
			switch (resAc) {
				case 'auth':
					// Handle authentication response
					const code = data.code;
					if (code === 0) {
						throw new Error('Authentication failed');
					} else {
						console.log(data.msg);
						this.startSubscription();
					}
					break;
				case 'pong':
					console.log('Heartbeat pong received');
					break;
				case 'subscribe':
					if (data.code === 0) {
						console.error('Subscription failed:', data.msg);
					} else {
						console.log(data.msg);
					}
					break;
				default:
					console.warn(`Unhandled response action: ${resAc}, message: ${data.msg}`);
			}
		} else if (data.data) {
			if (data.code !== 1) {
				console.error('Error sent in data:', data.msg);
				return;
			}

			let point;
			switch (data.data.type) {

				case 'quote':
					point = Point
						.measurement('quote')
						.setTag('symbol', data.data.s)
						.setTimestamp(data.data.t * 1_000_000)
						.setFloatField('quote',  data.data.ld);
					await this.influxDBClient.write(point);
					break;
				case 'depth':
					point = Point
						.measurement('depth')
						.setTag('symbol', data.data.s)
						.setTimestamp(data.data.t * 1_000_000)
						.setFloatField('buy', data.data.b.p)
						.setFloatField('sell', data.data.a.p);
					await this.influxDBClient.write(point);
					break;
				default:
					console.warn(`Unhandled data type: ${data.type}`);
			}
		} else {
			// Only message received
			console.log('Message received:', data);
		}
	}

	private async handleReconnect(): Promise<void> {
		if (this.retryCount < this.maxRetries) {
			const delay = Math.pow(2, this.retryCount) * 1000;
			this.retryCount++;
			setTimeout(() => this.connectWithRetry(), delay);
		}
	}

	private startSubscription() {
		console.log('Starting subscription to precious metal quotes.');
		const subscribeMessage = JSON.stringify({
			ac: 'subscribe',
			params: 'XAUCNY$GB,XAGCNY$GB',
			types: 'quote,depth',
		});

		if (this.activeWebSocket && this.activeWebSocket.readyState === WebSocket.OPEN) {
			try {
				this.activeWebSocket.send(subscribeMessage);
			} catch (error) {
				console.error('Failed to send heartbeat:', error);
				this.handleReconnect();
			}
		}
	}
}

export default {
	async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
		const id = env.PRECIOUS_METAL_QUOTE_LISTENER_DO.idFromName('ITickWebSocketManager');
		const obj = env.PRECIOUS_METAL_QUOTE_LISTENER_DO.get(id);
		ctx.waitUntil(obj.fetch(new Request('https://internal/init')));
	},

	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const id = env.PRECIOUS_METAL_QUOTE_LISTENER_DO.idFromName('ITickWebSocketManager');
		const obj = env.PRECIOUS_METAL_QUOTE_LISTENER_DO.get(id);
		return obj.fetch(request);
	},
} satisfies ExportedHandler<Env>;
